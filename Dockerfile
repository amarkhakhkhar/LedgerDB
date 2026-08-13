# Dependencies are installed in the build stage; runtime stays dependency-free.
FROM python:3.12-slim AS builder

WORKDIR /build
COPY requirements.txt .
RUN python -m pip wheel --wheel-dir /wheels -r requirements.txt
COPY ledgerdb ./ledgerdb
RUN python -m compileall -q ledgerdb

FROM python:3.12-slim AS runtime

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    LEDGERDB_DATA_DIR=/var/lib/ledgerdb \
    LEDGERDB_ROW='{"account":"cash","amount":100}'

COPY --from=builder /wheels /wheels
COPY requirements.txt .
RUN python -m pip install --no-cache-dir --no-index --find-links=/wheels -r requirements.txt && rm -rf /wheels
COPY --from=builder /build/ledgerdb ./ledgerdb

# Mount this path to retain both column files and the WAL across replacement.
VOLUME ["/var/lib/ledgerdb"]

ENTRYPOINT ["python", "-m", "ledgerdb.cli"]
CMD ["rows"]
