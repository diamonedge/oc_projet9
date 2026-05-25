FROM python:3.11-slim-bookworm

WORKDIR /app

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --frozen --no-dev

COPY producer/ ./producer/

CMD ["uv", "run", "python", "producer/ticket_producer.py"]
