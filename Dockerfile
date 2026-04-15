FROM python:3.11-slim AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
    gdal-bin libgdal-dev libpq-dev gcc g++ && rm -rf /var/lib/apt/lists/*
ENV GDAL_CONFIG=/usr/bin/gdal-config \
    CPLUS_INCLUDE_PATH=/usr/include/gdal \
    C_INCLUDE_PATH=/usr/include/gdal
WORKDIR /build
COPY requirements.txt .
RUN pip wheel --no-cache-dir --no-deps --wheel-dir /build/wheels -r requirements.txt

FROM python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    gdal-bin libgdal-dev libpq5 curl && rm -rf /var/lib/apt/lists/*
ENV GDAL_CONFIG=/usr/bin/gdal-config \
    CPLUS_INCLUDE_PATH=/usr/include/gdal \
    C_INCLUDE_PATH=/usr/include/gdal \
    PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=/app
WORKDIR /app
RUN useradd -m -u 1000 appuser
COPY --from=builder /build/wheels /wheels
RUN pip install --no-cache /wheels/* && rm -rf /wheels
COPY --chown=appuser:appuser . .
RUN for d in collector database alerts interface etl intelligence api; do \
    mkdir -p $d && touch $d/__init__.py; done && chown -R appuser:appuser /app
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:8000/api/health || exit 1
USER appuser
EXPOSE 8000
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
