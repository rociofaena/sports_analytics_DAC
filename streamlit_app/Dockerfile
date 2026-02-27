# Lightweight + stable
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Copy requirements first for caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY app.py .

# Elastic Beanstalk expects 8080
EXPOSE 8080
ENV PORT=8080

CMD ["streamlit","run","app.py","--server.address=0.0.0.0","--server.port=8501"]
