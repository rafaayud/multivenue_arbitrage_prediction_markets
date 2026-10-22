# syntax=docker/dockerfile:1

FROM python:3.13.17-slim

COPY --from=ghcr.io/astral-sh/uv:0.13.4 /uv /bin/uv

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src ./src
COPY migrations ./migrations

RUN uv sync --frozen --no-dev \
    && useradd --create-home app \
    && mkdir -p /app/data \
    && chown -R app:app /app

COPY docker/entrypoint.sh /entrypoint.sh
RUN sed -i 's/\r$//' /entrypoint.sh && chmod +x /entrypoint.sh

USER app

EXPOSE 8000 8001

ENTRYPOINT ["/entrypoint.sh"]
CMD ["/app/.venv/bin/uvicorn", "prediction_markets.api.control_main:app", "--host", "0.0.0.0", "--port", "8000"]
