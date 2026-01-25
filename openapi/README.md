# Spice.ai Go OpenAPI

Go OpenAPI client generated from the Spice.ai OpenAPI spec.

## Usage

Generate the client using the version specified in `doc.go`:

```bash
make client
```

Or override the version with an environment variable:

```bash
VERSION=trunk make client
VERSION=v1.11.0-rc.3 make client
```

The script will:
1. Download `openapi.json` from GitHub (tries tags first, then branches)
2. Generate Go client code using `openapi-generator-cli`
3. Clean up temporary files

## Requirements

- Docker (for running `openapi-generator-cli`)
- curl
