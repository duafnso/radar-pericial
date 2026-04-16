# Use Python 3.11 slim como base
FROM python:3.11-slim

# Environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    RAILWAY_HEALTHCHECK_PATH=/health

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gdal-bin \
    libgdal-dev \
    libpq-dev \
    gcc \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Work directory
WORKDIR /app

# Copy and install dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Create non-root user
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Copy application code
COPY --chown=appuser:appuser . .

# Ensure __init__.py exists for all packages
RUN for d in collector database alerts interface etl intelligence api; do \
    mkdir -p $d && touch $d/__init__.py; done

# Expose port
EXPOSE 8000

# ⚠️ REMOVIDO: HEALTHCHECK do Docker (conflita com Railway)
# Railway gerencia health checks via variáveis de ambiente

# Start command - formato exec para sinalização correta
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "info"]
