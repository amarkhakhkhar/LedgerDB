"""A small, explicit SQL parser and physical planner for LedgerDB.

The supported subset is SELECT, equality and BETWEEN WHERE predicates,
single-key GROUP BY with SUM/COUNT/AVG, and an equi-JOIN of ``ledger`` with
itself.  It intentionally rejects unsupported SQL instead of silently doing
something surprising.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from .engine import LedgerDB


class SQLSyntaxError(ValueError):
    pass


@dataclass(frozen=True)
class SelectItem:
    expression: str
    alias: str | None = None


@dataclass(frozen=True)
class Query:
    select: tuple[SelectItem, ...]
    table: str
    table_alias: str
    join_alias: str | None = None
    join_left: str | None = None
    join_right: str | None = None
    where_column: str | None = None
    where_op: str | None = None
    where_values: tuple[Any, ...] = ()
    group_by: str | None = None


def _literal(raw: str) -> Any:
    raw = raw.strip()
    if len(raw) >= 2 and raw[0] == raw[-1] == "'":
        return raw[1:-1].replace("''", "'")
    if re.fullmatch(r"-?\d+", raw):
        return int(raw)
    if re.fullmatch(r"-?(?:\d+\.\d*|\d*\.\d+)", raw):
        return float(raw)
    raise SQLSyntaxError(f"unsupported literal: {raw}")


def _split_select(raw: str) -> tuple[SelectItem, ...]:
    fields = []
    for item in raw.split(","):
        match = re.fullmatch(r"\s*(.+?)(?:\s+AS\s+([A-Za-z_]\w*))?\s*", item, re.I)
        if not match:
            raise SQLSyntaxError("invalid SELECT list")
        fields.append(SelectItem(match.group(1).strip(), match.group(2)))
    return tuple(fields)


def parse_sql(sql: str) -> Query:
    text = sql.strip().rstrip(";").strip()
    match = re.fullmatch(
        r"SELECT\s+(?P<select>.+?)\s+FROM\s+(?P<table>ledger)(?:\s+(?:AS\s+)?(?P<alias>[A-Za-z_]\w*))?"
        r"(?:\s+JOIN\s+ledger(?:\s+(?:AS\s+)?(?P<join_alias>[A-Za-z_]\w*))?\s+ON\s+(?P<join_left>[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?)\s*=\s*(?P<join_right>[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?))?"
        r"(?:\s+WHERE\s+(?P<where>.+?))?(?:\s+GROUP\s+BY\s+(?P<group>[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?))?",
        text, re.I,
    )
    if not match:
        raise SQLSyntaxError("supported SQL is SELECT ... FROM ledger [JOIN ledger ...] [WHERE ...] [GROUP BY ...]")
    alias = match.group("alias") or match.group("table")
    join_alias = match.group("join_alias")
    if match.group("join_left") and not join_alias:
        raise SQLSyntaxError("JOIN requires an alias")
    where_column = where_op = None
    values: tuple[Any, ...] = ()
    where = match.group("where")
    if where:
        between = re.fullmatch(r"([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?)\s+BETWEEN\s+(.+?)\s+AND\s+(.+)", where, re.I)
        equal = re.fullmatch(r"([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?)\s*=\s*(.+)", where, re.I)
        if between:
            where_column, where_op = between.group(1), "between"
            values = (_literal(between.group(2)), _literal(between.group(3)))
        elif equal:
            where_column, where_op = equal.group(1), "eq"
            values = (_literal(equal.group(2)),)
        else:
            raise SQLSyntaxError("WHERE supports only column = literal or column BETWEEN literal AND literal")
    return Query(_split_select(match.group("select")), "ledger", alias, join_alias,
                 match.group("join_left"), match.group("join_right"), where_column,
                 where_op, values, match.group("group"))


def _column(reference: str, alias: str) -> str:
    parts = reference.split(".")
    if len(parts) == 2 and parts[0] != alias:
        raise SQLSyntaxError(f"unknown table alias: {parts[0]}")
    return parts[-1]


class QueryPlanner:
    """Turns parsed SQL into physical operations on the existing engine."""

    def __init__(self, database: LedgerDB) -> None:
        self.database = database

    def explain(self, sql: str) -> list[str]:
        query = parse_sql(sql)
        steps = ["table_scan(ledger)"]
        if query.join_alias:
            steps.append("sort_merge_join(ledger, ledger)")
        if query.where_op == "between" and not query.join_alias:
            steps.append(f"range_index_binary_search({_column(query.where_column or '', query.table_alias)})")
        elif query.where_op == "eq" and not query.join_alias:
            steps.append(f"equality_index({_column(query.where_column or '', query.table_alias)})")
        if query.group_by:
            steps.append("hash_group_by")
        steps.append("project")
        return steps

    def execute(self, sql: str) -> list[dict[str, Any]]:
        query = parse_sql(sql)
        if query.join_alias:
            rows = self._join_rows(query)
        elif query.where_op == "between":
            rows = self.database.filter_between(_column(query.where_column or "", query.table_alias), *query.where_values)
        elif query.where_op == "eq":
            rows = self.database.filter_eq(_column(query.where_column or "", query.table_alias), query.where_values[0])
        else:
            rows = self.database.rows()
        if query.group_by:
            return self._group(rows, query)
        return [self._project(row, query.select, query.table_alias, query.join_alias) for row in rows]

    def _join_rows(self, query: Query) -> list[dict[str, Any]]:
        assert query.join_alias and query.join_left and query.join_right
        left = _column(query.join_left, query.table_alias)
        right = _column(query.join_right, query.join_alias)
        rows = self.database.sort_merge_join(self.database, left, right)
        if not query.where_op:
            return rows
        prefix = "left" if (query.where_column or "").split(".")[0] == query.table_alias else "right"
        column = (query.where_column or "").split(".")[-1]
        if query.where_op == "eq":
            return [row for row in rows if row[f"{prefix}.{column}"] == query.where_values[0]]
        return [row for row in rows if query.where_values[0] <= row[f"{prefix}.{column}"] <= query.where_values[1]]

    def _group(self, rows: list[dict[str, Any]], query: Query) -> list[dict[str, Any]]:
        group_column = _column(query.group_by or "", query.table_alias)
        aggregates = [item for item in query.select if re.fullmatch(r"(?:SUM|COUNT|AVG)\(.+\)", item.expression, re.I)]
        if len(aggregates) != 1:
            raise SQLSyntaxError("GROUP BY requires exactly one SUM, COUNT, or AVG aggregate")
        aggregate = aggregates[0]
        func, raw_value = re.fullmatch(r"(SUM|COUNT|AVG)\((.+)\)", aggregate.expression, re.I).groups()
        value_column = _column(raw_value.strip(), query.table_alias)
        # For an unfiltered single table aggregation, use LedgerDB's existing
        # open-addressed hash aggregation implementation as the physical node.
        if not query.join_alias and query.where_op is None:
            result = self.database.group_by(group_column, value_column)
            name = aggregate.alias or f"{func.lower()}({value_column})"
            values = {"SUM": result.sums, "COUNT": result.counts, "AVG": result.avgs}[func.upper()]
            return [{group_column: int(key), name: value.item() if hasattr(value, "item") else value}
                    for key, value in zip(result.keys, values, strict=True)]
        grouped: dict[Any, list[float]] = {}
        for row in rows:
            key = row[group_column]
            grouped.setdefault(key, []).append(float(row[value_column]))
        name = aggregate.alias or f"{func.lower()}({value_column})"
        return [{group_column: key, name: (len(values) if func.upper() == "COUNT" else sum(values) if func.upper() == "SUM" else sum(values) / len(values))}
                for key, values in sorted(grouped.items())]

    @staticmethod
    def _project(row: dict[str, Any], select: tuple[SelectItem, ...], alias: str, join_alias: str | None) -> dict[str, Any]:
        if len(select) == 1 and select[0].expression == "*":
            return dict(row)
        result = {}
        for item in select:
            ref = item.expression
            parts = ref.split(".")
            if join_alias and len(parts) == 2:
                prefix = "left" if parts[0] == alias else "right" if parts[0] == join_alias else None
                if prefix is None:
                    raise SQLSyntaxError(f"unknown table alias: {parts[0]}")
                key = f"{prefix}.{parts[1]}"
            else:
                key = _column(ref, alias)
            result[item.alias or ref] = row[key]
        return result
