FROM node:22-slim AS frontend-build

WORKDIR /app

COPY package.json package-lock.json tsconfig.json vite.config.ts index.html ./
COPY frontend ./frontend
COPY interface ./interface

RUN npm ci && npm run frontend:build


FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    gdal-bin libgdal-dev libpq-dev gcc g++ curl \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app

COPY --chown=appuser:appuser api ./api
COPY --chown=appuser:appuser alerts ./alerts
COPY --chown=appuser:appuser collector ./collector
COPY --chown=appuser:appuser database ./database
COPY --chown=appuser:appuser etl ./etl
COPY --chown=appuser:appuser intelligence ./intelligence
COPY --chown=appuser:appuser run_collect.py working_data_collector.py ./
COPY --chown=appuser:appuser --from=frontend-build /app/interface/templates ./interface/templates
COPY --chown=appuser:appuser --from=frontend-build /app/interface/static ./interface/static

RUN for d in collector database alerts interface etl intelligence api; do \
    mkdir -p "$d" && touch "$d/__init__.py"; \
    done

USER appuser

EXPOSE 8000

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "info"]
