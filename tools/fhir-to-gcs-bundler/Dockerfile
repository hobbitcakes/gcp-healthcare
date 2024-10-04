ARG PYTHON_VERSION=3.12.6
FROM python:${PYTHON_VERSION}-slim as base

ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Copy the source code into the container.
COPY . .

RUN python -m pip install -r requirements.txt

# Expose the port that the application listens on.
EXPOSE 8080

# Run the application.
CMD exec gunicorn --bind :8080 --workers 1 --threads 8 main:app
