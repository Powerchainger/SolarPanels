# Multi-stage build for solar-scraper
# Stage 1: Build dependencies
FROM python:3.13-slim AS builder

WORKDIR /build

COPY pyproject.toml uv.lock ./

RUN pip install --no-cache-dir uv \
    && uv sync --no-dev --no-install-project

COPY . .

RUN uv sync --no-dev

# Stage 2: Runtime
FROM python:3.13-slim AS runtime

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /build/.venv /app/.venv

# Copy application code
COPY --from=builder /build /app

# Add venv to PATH
ENV PATH="/app/.venv/bin:$PATH"

# Create non-root user
RUN useradd --create-home --shell /bin/bash app
USER app

# Health check: simple Python check that can import the main module
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "import main; print('ok')" || exit 1

ENTRYPOINT ["python", "main.py"]
