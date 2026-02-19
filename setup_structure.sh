#!/bin/bash
# Create ClipForge directory structure
mkdir -p src/{discovery,download,transcribe,decide,render,package,db,models,utils}
mkdir -p outputs
mkdir -p assets
mkdir -p tests
touch src/__init__.py
touch src/discovery/__init__.py
touch src/download/__init__.py
touch src/transcribe/__init__.py
touch src/decide/__init__.py
touch src/render/__init__.py
touch src/package/__init__.py
touch src/db/__init__.py
touch src/models/__init__.py
touch src/utils/__init__.py
echo "✅ Structure created"
