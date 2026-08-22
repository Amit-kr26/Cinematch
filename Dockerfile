# ── React build ───────────────────────────────────────────────────────────────
FROM node:20-alpine AS ui
WORKDIR /ui
COPY services/frontend/package.json services/frontend/package-lock.json ./
RUN npm ci
COPY services/frontend/ ./
RUN npm run build

# ── Runtime: FastAPI + SQLite, no JVM/Spark (seed ships pre-trained) ─────────
FROM python:3.12-slim
WORKDIR /app

RUN pip install --no-cache-dir \
    "fastapi>=0.111" \
    "uvicorn[standard]>=0.30" \
    "prometheus-client>=0.20" \
    "pydantic>=2.7" \
    "httpx>=0.27" \
    "numpy>=1.26"

COPY app/ ./app/
COPY --from=ui /ui/dist ./app/static

ENV DB_PATH=/data/app.db \
    DATA_DIR=/data \
    PORT=7860 \
    PYTHONUNBUFFERED=1

EXPOSE 7860
CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-7860}"]
