FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml .
COPY civitas/ civitas/
COPY examples/ examples/

RUN pip install --no-cache-dir -e ".[nats]" && \
    useradd --create-home --uid 1000 civitas

# Never run agents as root — a compromised agent process would own the container.
USER civitas

ENTRYPOINT ["civitas", "run"]
