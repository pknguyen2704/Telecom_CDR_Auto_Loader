FROM python:3.11-slim
# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Set working directory
WORKDIR /app
COPY pyproject.toml ./
RUN uv pip install --system --no-cache -r pyproject.toml
COPY src/ ./src/

RUN mkdir -p auto_loader

ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "src.main"]

