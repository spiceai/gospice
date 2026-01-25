#!/bin/bash

# Generate Go client from OpenAPI spec using Docker

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMP_DIR="$SCRIPT_DIR/.gen_tmp"
PACKAGE_NAME="oasgospice"

# Use VERSION env var if set, otherwise extract from doc.go
if [ -z "$VERSION" ]; then
  VERSION=$(sed -n 's/^var Version = "\(.*\)"$/\1/p' "$SCRIPT_DIR/doc.go")
fi
if [ -z "$VERSION" ]; then
  echo "Error: Could not determine version from VERSION env var or doc.go"
  exit 1
fi

# Download openapi.json from GitHub
# Try tags first (for versions like v1.11.0-rc.3), then branches (for trunk, main, etc.)
OPENAPI_URL_TAG="https://raw.githubusercontent.com/spiceai/spiceai/refs/tags/${VERSION}/.schema/openapi.json"
OPENAPI_URL_BRANCH="https://raw.githubusercontent.com/spiceai/spiceai/refs/heads/${VERSION}/.schema/openapi.json"

echo "Downloading OpenAPI spec for version: $VERSION"
if curl -fsSL "$OPENAPI_URL_TAG" -o "$SCRIPT_DIR/openapi.json" 2>/dev/null; then
  echo "Downloaded from tag: $OPENAPI_URL_TAG"
elif curl -fsSL "$OPENAPI_URL_BRANCH" -o "$SCRIPT_DIR/openapi.json" 2>/dev/null; then
  echo "Downloaded from branch: $OPENAPI_URL_BRANCH"
else
  echo "Error: Failed to download openapi.json from tag or branch"
  exit 1
fi

# Clean up temp dir
rm -rf "$TEMP_DIR"
mkdir -p "$TEMP_DIR"

# TODO: remove (--skip-validate-spec) once the spec is fixed

docker run --rm \
  -v "$SCRIPT_DIR":/local \
  openapitools/openapi-generator-cli generate \
  -i /local/openapi.json \
  -g go \
  -o /local/.gen_tmp \
  --skip-validate-spec \
  --global-property=models,modelDocs=false,supportingFiles=utils.go \
  --additional-properties=enumClassPrefix=true,packageName=$PACKAGE_NAME

# Clean up unwanted generated files/folders
rm -rf "$TEMP_DIR/api" "$TEMP_DIR/.openapi-generator"
rm -f "$SCRIPT_DIR/openapi.json"

# Combine all model_*.go files into models.gen.go
echo "package $PACKAGE_NAME" > "$TEMP_DIR/models.gen.go"
echo "" >> "$TEMP_DIR/models.gen.go"

# Collect all imports from model files
imports=""
for f in "$TEMP_DIR"/model_*.go; do
  [ -f "$f" ] || continue
  # Extract import lines (between "import (" and ")")
  imports="$imports$(sed -n '/^import ($/,/^)$/p' "$f" | grep -v '^import ($' | grep -v '^)$')"$'\n'
done

# Deduplicate and write imports
if [ -n "$imports" ]; then
  echo "import (" >> "$TEMP_DIR/models.gen.go"
  echo "$imports" | sort -u | grep -v '^$' >> "$TEMP_DIR/models.gen.go"
  echo ")" >> "$TEMP_DIR/models.gen.go"
  echo "" >> "$TEMP_DIR/models.gen.go"
fi

# Append model contents (skip package and import lines)
for f in "$TEMP_DIR"/model_*.go; do
  [ -f "$f" ] || continue
  echo "// --- $(basename "$f") ---" >> "$TEMP_DIR/models.gen.go"
  # Skip package line and import block, keep the rest
  sed '/^package /d; /^import ($/,/^)$/d; /^import "/d' "$f" >> "$TEMP_DIR/models.gen.go"
  echo "" >> "$TEMP_DIR/models.gen.go"
done

# Remove individual model files
rm -f "$TEMP_DIR"/model_*.go

# Rename utils.go to utils.gen.go
if [ -f "$TEMP_DIR/utils.go" ]; then
  mv "$TEMP_DIR/utils.go" "$TEMP_DIR/utils.gen.go"
fi

# Append custom type fixes from utils_fix.go (skip package/import lines)
if [ -f "$SCRIPT_DIR/utils_fix.go" ]; then
  tail -n +4 "$SCRIPT_DIR/utils_fix.go" >> "$TEMP_DIR/utils.gen.go"
fi

# Remove old generated files from target dir
rm -f "$SCRIPT_DIR"/*.gen.go

# Move generated files to target dir
mv "$TEMP_DIR"/*.gen.go "$SCRIPT_DIR/"

# Clean up
rm -rf "$TEMP_DIR"

echo "Generation complete!"
