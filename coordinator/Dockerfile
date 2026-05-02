FROM python:3.12-slim
WORKDIR /app
RUN pip install fastapi uvicorn httpx --no-cache-dir
COPY . .
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
