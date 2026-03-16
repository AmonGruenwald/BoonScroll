FROM python:3.12-slim

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ libffi-dev && \
    rm -rf /var/lib/apt/lists/*

# Python deps
COPY backend/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# App code
COPY backend/ /app/backend/
COPY frontend/ /app/frontend/

# Data volume
RUN mkdir -p /data

ENV PYTHONPATH=/app/backend
ENV FRONTEND_DIR=/app/frontend
ENV DB_PATH=/data/boonscroll.db
ENV PORT=8000

EXPOSE 8000

CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000"]
