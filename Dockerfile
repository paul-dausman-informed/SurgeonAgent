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
COPY agent.py server.py research.py profile_generator.py ./
COPY static/ ./static/
COPY SurgeonScores/ ./SurgeonScores/
COPY NationalTop80Score.csv ./

# Create directories for runtime data
RUN mkdir -p output research_cache

# Railway injects PORT env var; default to 8000 for local dev
ENV PORT=8000
EXPOSE ${PORT}

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:${PORT}/health || exit 1

# Railway start command is in railway.json; this is the fallback
CMD uvicorn server:app --host 0.0.0.0 --port ${PORT} --workers 1
