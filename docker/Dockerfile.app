FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/workspace
ENV PIP_DEFAULT_TIMEOUT=600

WORKDIR /workspace

RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless build-essential procps \
    && rm -rf /var/lib/apt/lists/*

COPY docker/requirements-app.txt /tmp/requirements-app.txt
RUN pip install --no-cache-dir --default-timeout=600 -r /tmp/requirements-app.txt

COPY . .

EXPOSE 8501 8000
