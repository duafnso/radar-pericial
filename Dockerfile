FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    gdal-bin libgdal-dev libpq-dev gcc g++ curl \
    && rm -rf /var/lib/apt/lists/* && apt-get clean

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Copia TODO o código (incluindo interface/templates/)
COPY --chown=appuser:appuser . .

# Cria __init__.py se não existir
RUN for d in collector database alerts interface etl intelligence api; do \
    mkdir -p $d && touch $d/__init__.py; done

# Verifica se index.html existe
RUN if [ -f "interface/templates/index.html" ]; then \
      echo "✅ index.html encontrado"; \
    else \
      echo "❌ index.html NÃO encontrado"; \
      ls -la interface/templates/ || echo "Pasta não existe"; \
    fi

EXPOSE 8000

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "info"]
