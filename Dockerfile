FROM eclipse-temurin:17-jre-noble

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen

COPY data/ data/
COPY src/ src/
COPY tests/ tests/

CMD ["uv", "run", "python", "src/main.py"]
