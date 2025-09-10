FROM python:3.12-alpine

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY solarlog2mqtt ./
COPY solarlog2mqtt_core ./solarlog2mqtt_core

ENV PYTHONUNBUFFERED=1

CMD ["python", "./solarlog2mqtt"]
