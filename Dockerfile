FROM python:3.11-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /workspace
COPY pyproject.toml README.md ./
COPY src/ ./src/
RUN pip install --no-cache-dir -e '.[dev,spark,analytics,ml]'
CMD ["bash"]
