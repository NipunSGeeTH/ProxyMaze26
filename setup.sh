#!/usr/bin/env bash
# ProxyMaze'26 — one-shot setup
set -e

echo "==> Creating virtual environment..."
python3 -m venv venv

echo "==> Activating venv and installing dependencies..."
source venv/bin/activate
pip install --upgrade pip -q
pip install -r requirements.txt -q

echo ""
echo "✅  Setup complete!"
echo ""
echo "To start the server:"
echo "  source venv/bin/activate"
echo "  uvicorn main:app --host 0.0.0.0 --port 8000 --reload"
echo ""
echo "Or just run:  ./run.sh"
