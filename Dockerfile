# Dependencies are installed in the build stage; runtime stays dependency-free.
FROM python:3.12-slim AS builder

WORKDIR /build
COPY ledgerdb ./ledgerdb
RUN python -m compileall -q ledgerdb

FROM python:3.12-slim AS runtime

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    LEDGERDB_DATA_DIR=/var/lib/ledgerdb

COPY --from=builder /build/ledgerdb ./ledgerdb

# Mount this path to retain both column files and the WAL across replacement.
VOLUME ["/var/lib/ledgerdb"]

ENTRYPOINT ["python", "-m", "ledgerdb.cli"]
CMD ["rows"]
