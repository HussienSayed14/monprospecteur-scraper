FROM python:3.12-slim

# Set timezone to Toronto
ENV TZ=America/Toronto
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Install Docker CLI so scheduler can trigger the scraper container
RUN apt-get update && apt-get install -y \
    curl \
    ca-certificates \
    && curl -fsSL https://download.docker.com/linux/static/stable/x86_64/docker-24.0.7.tgz \
    | tar -xz --strip-components=1 -C /usr/local/bin docker/docker \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY scheduler.py .

RUN mkdir -p output/logs

CMD ["python", "scheduler.py"]