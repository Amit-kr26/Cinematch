FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir "kafka-python-ng>=2.2" "numpy>=1.26"
RUN adduser --disabled-password --gecos "" appuser
USER appuser
COPY simulator.py .
CMD ["tail", "-f", "/dev/null"]
