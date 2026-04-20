# syntax=docker/dockerfile:1.7

FROM node:20-alpine AS dashboard-builder
WORKDIR /build/dashboard

COPY dashboard/package.json dashboard/pnpm-lock.yaml ./
RUN corepack enable && pnpm install --frozen-lockfile

COPY dashboard/ ./
RUN pnpm build

FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY shinbot/ ./shinbot/
COPY main.py ./main.py
RUN pip install --upgrade pip \
    && pip install .

COPY config.example.toml ./config.example.toml
RUN cp config.example.toml config.toml

COPY --from=dashboard-builder /build/dashboard/dist ./dashboard/dist

RUN mkdir -p data/db data/plugins data/plugin_data data/sessions data/audit data/temp

EXPOSE 3945

CMD ["python", "main.py", "--config", "config.toml", "--api-host", "0.0.0.0", "--api-port", "3945"]
