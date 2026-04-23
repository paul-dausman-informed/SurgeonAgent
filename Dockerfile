# ============================================================
# SurgeonAgent — Docker image for Railway deployment
# ============================================================
# Requires both Python (app + libraries) and Node.js (claude CLI)
# because claude_agent_sdk spawns the claude CLI as a subprocess.

FROM python:3.12-slim

# Install Node.js 20 (needed for claude CLI subprocess)
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates && \
    curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y --no-install-recommends nodejs && \
    rm -rf /var/lib/apt/lists/*

# Install Claude CLI globally
RUN npm install -g @anthropic-ai/claude-code

# Set working directory
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code and data
COPY agent.py server.py start.py research.py profile_generator.py summary_generator.py email_sender.py palantir_score.py ./
COPY static/ ./static/
COPY SurgeonScores/ ./SurgeonScores/
COPY NationalTop80Score.csv ./
COPY cbsa_lookup.json ./
COPY rules/ ./rules/
COPY knowledge/ ./knowledge/

# Create directories for runtime data
RUN mkdir -p output research_cache

# Railway injects PORT env var; default to 8000 for local dev
ENV PORT=8000

# No Docker HEALTHCHECK — let Railway handle it via railway.json
# Start the server (railway.json startCommand overrides this)
CMD ["python", "start.py"]
