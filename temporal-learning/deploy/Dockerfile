# One image, many roles: the command chooses the Worker type or the API.
FROM python:3.11-slim

WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir .

RUN useradd --create-home temporal && mkdir /data && chown temporal /data
USER temporal
WORKDIR /data

CMD ["python", "-m", "capstone.worker", "--role", "orchestration"]
