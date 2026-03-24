FROM mcr.microsoft.com/playwright/python:v1.51.0-noble

# Set timezone to Toronto
ENV TZ=America/Toronto
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Chromium only — skip Firefox and WebKit to keep image small
# --with-deps installs only the system libs needed for Chromium
RUN playwright install chromium --with-deps

# Copy all scraper files
COPY main.py .
COPY excel_uploader.py .
COPY drive_uploader.py .
COPY sheets_uploader.py .
COPY email_sender.py .
COPY run_history.py .
COPY logger.py .
COPY google_auth.py .

# Output directories (volumes mounted over these at runtime)
RUN mkdir -p output/pdfs output/prints output/data output/failed output/logs

CMD ["python", "main.py"]