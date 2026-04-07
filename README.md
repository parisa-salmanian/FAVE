# New Builds Explorer (OSM-based)

## Dev setup (macOS)
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

## Run
Terminal A:
uvicorn api.server:app --host 127.0.0.1 --port 8001 --reload

Terminal B:
cd frontend
python3 -m http.server 5500

Terminal C(any directory):
ollama serve (one time only)
ollama pull llama3.2

App: http://127.0.0.1:5500/index.html
Health: http://127.0.0.1:8001/health
