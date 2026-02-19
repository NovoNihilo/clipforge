#!/bin/bash
# ═══════════════════════════════════════════════════════════
# ClipForge — Full Setup Script for macOS
# Copy-paste this ENTIRE script into your terminal.
# ═══════════════════════════════════════════════════════════

echo "🔧 Step 1: Install FFmpeg via Homebrew"
brew install ffmpeg

echo ""
echo "🔧 Step 2: Create project directory"
mkdir -p ~/clipforge && cd ~/clipforge

echo ""
echo "🔧 Step 3: Create Python virtual environment"
python3 -m venv .venv
source .venv/bin/activate

echo ""
echo "🔧 Step 4: Check Python version (need 3.11+)"
python --version

echo ""
echo "✅ Setup script complete. Now follow the manual steps in the instructions."
