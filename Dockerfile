FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ridematch ./ridematch
ENV PYTHONUNBUFFERED=1
# docker-compose picks the service's command
CMD ["gunicorn", "-b", "0.0.0.0:5000", "-w", "2", "ridematch.producer:app"]
