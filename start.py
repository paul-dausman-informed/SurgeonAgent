"""Startup script that reads PORT from environment and launches uvicorn."""
import os
import uvicorn

port = int(os.environ.get("PORT", 8000))
print(f"Starting SurgeonAgent on port {port}")
uvicorn.run("server:app", host="0.0.0.0", port=port, workers=1)
