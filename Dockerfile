# DataPulse v5 — Production Dockerfile
# Railway: builder="DOCKERFILE" (railway.toml)
FROM python:3.11-slim

# System dependencies: Tesseract OCR + Playwright + libmagic
RUN apt-get update && apt-get install -y --no-install-recommends \
    tesseract-ocr \
    tesseract-ocr-tur \
    libmagic1 \
    libglib2.0-0 \
    libnss3 \
    libnspr4 \
    libdbus-1-3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxcb1 \
    libxkbcommon0 \
    libx11-6 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libpango-1.0-0 \
    libcairo2 \
    libasound2 \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps first (layer cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Playwright: install Chromium only (smallest footprint)
RUN playwright install chromium --with-deps || echo "Playwright install skipped"

# Copy application code
COPY . .

# PYTHONPATH=/app is critical — "from backend.xxx" imports
ENV PYTHONPATH=/app
ENV PORT=8000

EXPOSE 8000

# Railway injects $PORT; fallback to 8000
CMD ["sh", "-c", "uvicorn backend.app:app --host 0.0.0.0 --port ${PORT:-8000}"]
