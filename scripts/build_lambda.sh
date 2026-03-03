#!/usr/bin/env bash
# Packages the catalog_sync module + dependencies into a Lambda deployment zip.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DIST_DIR="$PROJECT_ROOT/dist"
BUILD_DIR="$PROJECT_ROOT/.build/lambda"

echo "==> Cleaning build directory"
rm -rf "$BUILD_DIR" "$DIST_DIR/lambda.zip"
mkdir -p "$BUILD_DIR" "$DIST_DIR"

echo "==> Installing dependencies"
pip install \
  --target "$BUILD_DIR" \
  --platform manylinux2014_x86_64 \
  --only-binary=:all: \
  --implementation cp \
  --python-version 3.11 \
  pyiceberg databricks-sdk boto3 pydantic 2>/dev/null || \
pip install --target "$BUILD_DIR" pyiceberg databricks-sdk boto3 pydantic

echo "==> Copying source code"
cp -r "$PROJECT_ROOT/catalog_sync" "$BUILD_DIR/"

echo "==> Creating zip"
cd "$BUILD_DIR"
zip -r "$DIST_DIR/lambda.zip" . -x '*.pyc' '__pycache__/*' '*.dist-info/*'

echo "==> Built: $DIST_DIR/lambda.zip ($(du -h "$DIST_DIR/lambda.zip" | cut -f1))"
