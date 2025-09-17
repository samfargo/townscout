#!/bin/bash

# TownScout MVP Quick Start Script

echo "🏙️  TownScout MVP Setup"
echo "====================="

# Check if virtual environment exists
if [ ! -d ".venv" ]; then
    echo "📦 Creating virtual environment..."
    if command -v python3.12 >/dev/null 2>&1; then
        python3.12 -m venv .venv
    else
        python3 -m venv .venv
    fi
fi

echo "🔧 Activating virtual environment..."
source .venv/bin/activate

echo "📥 Installing dependencies..."
pip install -r requirements.txt

echo "📁 Creating data directories..."
mkdir -p data/osm data/poi data/minutes data/deltas data/osmnx_cache state_tiles tiles/legends

echo "📄 Copying environment file..."
if [ ! -f ".env" ]; then
    cp .env.example .env
    echo "⚠️  Please edit .env with your tile URLs before deployment"
fi

echo "✅ Setup complete!"
echo ""
echo "Next steps:"
echo "1. Run the pipeline: make all"
echo "2. Start the API: uvicorn api.main:app --reload"
echo "3. Open tiles/web/index.html in a browser"
echo ""
echo "For parallel processing: make minutes-par"
echo "For help: make help or check README.md" 
