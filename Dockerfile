FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src/ ./src/
COPY pyproject.toml .
ENV PYTHONPATH=/app/src BILLING_DB_PATH=/data/billing.db BILLING_RAW_LOG_DIR=/data/raw-log
VOLUME ["/data"]
EXPOSE 8000
CMD ["uvicorn", "billing_engine.api:app", "--host", "0.0.0.0", "--port", "8000"]
