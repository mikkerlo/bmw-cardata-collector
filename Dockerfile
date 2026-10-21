# syntax=docker/dockerfile:1
# Build the wheel in a throwaway stage so no build tooling ships in the image.
FROM python:3.12.12-slim-trixie AS build

WORKDIR /build
RUN pip install --no-cache-dir "hatchling==1.27.0"
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN python -m hatchling build --target wheel

FROM python:3.12.12-slim-trixie AS runtime

LABEL org.opencontainers.image.title="bmw-cardata-collector" \
      org.opencontainers.image.description="Durable archival collector for BMW CarData MQTT streaming and REST snapshots" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.source="https://github.com/mikkerlo/bmw-cardata-collector"

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    COLLECTOR_CONFIG=/config/config.yaml

# Pinned runtime dependencies, resolved in requirements.txt.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt

COPY --from=build /build/dist/*.whl /tmp/
RUN pip install --no-cache-dir --no-deps /tmp/*.whl && rm /tmp/*.whl

# Non-root. The /data volume must be writable by this uid — see README.
RUN useradd --create-home --uid 10001 --shell /usr/sbin/nologin collector \
 && mkdir -p /data /config \
 && chown -R collector:collector /data \
 && chmod 700 /data
USER collector
WORKDIR /home/collector
VOLUME ["/data"]
EXPOSE 9090

# Liveness only: a parked car sends nothing for days, which is not a fault.
# Readiness is richer and lives at /health/ready.
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:9090/health/live', timeout=4).status==200 else 1)"

ENTRYPOINT ["collector"]
CMD ["run"]
