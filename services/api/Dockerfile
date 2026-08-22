FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml .
RUN pip install --no-cache-dir .
RUN adduser --disabled-password --gecos "" appuser
USER appuser
COPY . .
EXPOSE 8000
CMD ["python", "main.py"]
