FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY worker_bot.py .

EXPOSE 8081

CMD ["python", "worker_bot.py"]
