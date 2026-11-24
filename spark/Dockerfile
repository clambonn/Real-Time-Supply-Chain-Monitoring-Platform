FROM python:3.13-slim-bookworm

RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

RUN pip install --no-cache-dir pyspark==4.3.0

WORKDIR /workspace

CMD ["python", "read_and_validate.py"]