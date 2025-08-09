# Tigris MCAP Benchmark

Extract messages from a local MCAP file, write each message as a separate binary file, upload each object individually to an S3 bucket, and record per-file upload timings and a summary. A companion download benchmark fetches objects from S3 (optionally under a prefix) and records first-byte latency and total download time.

## Table of Contents
- [Overview](#overview)
- [Requirements](#requirements)
- [Install](#install)
- [Upload CLI](#upload-cli)
  - [Usage](#usage)
  - [Examples](#examples)
  - [Outputs](#outputs)
  - [Notes](#notes)
- [Download CLI](#download-cli)
  - [Usage](#usage-1)
  - [Examples](#examples-1)
  - [Outputs](#outputs-1)
  - [Notes](#notes-1)
- [Using 1Password for Credentials](#using-1password-for-credentials)
  - [Upload CLI (direct refs)](#upload-cli-direct-refs)
  - [Download CLI (env vars)](#download-cli-env-vars)
- [S3 Defaults](#s3-defaults)

## Overview
This project provides two CLI tools:
- `tigris-mcap-benchmark`: Extracts messages from an MCAP file and uploads each as an object to S3, measuring per-file upload times and throughput.
- `download-benchmark`: Lists and downloads objects from S3 (optional prefix) to a local directory, measuring time-to-first-byte and total download time.

## Requirements
- Python 3.12+
- Poetry
- AWS credentials configured (env vars, `~/.aws/credentials`, or `--profile`)

## Install
1. `poetry install`
2. Ensure dependencies are installed and AWS credentials are available.

## Upload CLI

### Usage
`poetry run tigris-mcap-benchmark <mcap> [bucket] [--bucket NAME] [--prefix PREFIX] [--out-dir DIR] [--start N] [--limit N] [--region REGION] [--profile PROFILE] [--endpoint-url URL] [--path-style] [--cleanup] [--concurrency N] [--op-access REF] [--op-secret REF] [--op-session-token REF] [--csv PATH]`

### Examples
- Extract all messages, upload under prefix, keep local files:
  `poetry run tigris-mcap-benchmark /path/to/file.mcap my-bucket --prefix test/run1`

- Process only 500 messages starting at index 1000, delete local files after upload:
  `poetry run tigris-mcap-benchmark /path/to/file.mcap my-bucket --start 1000 --limit 500 --cleanup`

- Upload concurrently with 16 workers (asyncio + threads):
  `poetry run tigris-mcap-benchmark /path/to/file.mcap my-bucket --prefix test/run2 --concurrency 16`

- Use an S3-compatible endpoint (e.g., MinIO):
  `poetry run tigris-mcap-benchmark /path/to/file.mcap my-bucket --endpoint-url http://localhost:9000 --path-style --region us-east-1`

- Read AWS keys from 1Password (requires `op` CLI, already signed in). You can pass 1Password references directly to the CLI:
  `poetry run tigris-mcap-benchmark /path/to/file.mcap --bucket my-bucket \
    --op-access "op://My Vault/AWS Keys/username" \
    --op-secret "op://My Vault/AWS Keys/credential" \
    --op-session-token "op://My Vault/AWS Keys/session"`

### Outputs
- Extracted message binaries in `--out-dir` (default `./mcap_messages_out`), named like `00000042__ch3__topic_name__log1712593492000000000.bin`.
- Per-file upload lines printed to stdout with size, duration, and Mb/s.
- Summary line with totals and averages.
- CSV of per-file results at `--csv` path (default `<out-dir>/upload_results.csv`).

### Notes
- Upload timing measures only the S3 transfer call, not extraction or file I/O.
- With `--concurrency > 1`, uploads run concurrently using `asyncio.to_thread` around the blocking boto3 upload.
- Credentials and region resolution follow standard Boto3 behavior unless overridden via flags.
- For S3-compatible storage, use `--endpoint-url` and optionally `--path-style` if the provider requires path-style buckets.
- Bucket name resolution order: `--bucket` flag > positional `bucket` > env `S3_BUCKET` > `src/tigris_mcap_benchmark/config.py:DEFAULT_BUCKET`.
- 1Password references use `op read op://...` under the hood. Ensure the `op` CLI is installed and you are signed in (`op signin`).

## Download CLI

### Usage
`poetry run download-benchmark [bucket] [--bucket NAME] [--prefix PREFIX] [--limit N] [--region REGION] [--profile PROFILE] [--endpoint-url URL] [--path-style] [--tmp-dir DIR] [--csv PATH] [--concurrency N]`

### Examples
- Download all objects under a prefix to a temporary directory:
  `poetry run download-benchmark my-bucket --prefix test/run1`

- Limit to 200 objects and write a CSV of results:
  `poetry run download-benchmark my-bucket --prefix test/run1 --limit 200 --csv downloads.csv`

- Download concurrently with 16 workers:
  `poetry run download-benchmark my-bucket --prefix test/run1 --concurrency 16`

- Use an S3-compatible endpoint (e.g., MinIO):
  `poetry run download-benchmark my-bucket --endpoint-url http://localhost:9000 --path-style --region us-east-1`

### Outputs
- Files downloaded under a temporary directory (auto-created) or `--tmp-dir` if provided, mirroring S3 keys.
- Per-file lines with first-byte time, total time, size, and Mb/s.
- Summary line with totals and averages.
- CSV of per-file results at `--csv` path when provided.

### Notes
- Credentials for the download benchmark are taken from the environment or profile (it does not take `--op-*` 1Password flags). See the 1Password section below for ways to set env vars via `op`.
- Bucket name resolution is the same as the upload CLI: `--bucket` flag > positional `bucket` > env `S3_BUCKET` > `src/tigris_mcap_benchmark/config.py:DEFAULT_BUCKET`.

## Using 1Password for Credentials
- Sign in: `op signin` (ensure the shell has an active session).

### Upload CLI (direct refs)
- Pass `--op-access`, `--op-secret`, and optional `--op-session-token` with `op://...` references as shown above.

### Download CLI (env vars)
- Set standard AWS env vars from 1Password before running the command. Two common approaches:
  - Export with `op read`:
    - `export AWS_ACCESS_KEY_ID="$(op read 'op://My Vault/AWS Keys/username')"`
    - `export AWS_SECRET_ACCESS_KEY="$(op read 'op://My Vault/AWS Keys/credential')"`
    - `export AWS_SESSION_TOKEN="$(op read 'op://My Vault/AWS Keys/session')"`  # if applicable
    - Then run: `poetry run download-benchmark my-bucket --prefix test/run1`
  - Use `op run` with an env file (if you maintain one):
    - Create a file like `.env.op` with lines (a starter template is provided as `.env.op.example`):
      - `AWS_ACCESS_KEY_ID=op://My Vault/AWS Keys/username`
      - `AWS_SECRET_ACCESS_KEY=op://My Vault/AWS Keys/credential`
      - `AWS_SESSION_TOKEN=op://My Vault/AWS Keys/session`  # optional
    - Copy the template and run under 1Password:
      - `cp .env.op.example .env.op`
      - `op run --env-file .env.op -- poetry run download-benchmark my-bucket --prefix test/run1`
      - (Optional, upload via env vars) `op run --env-file .env.op -- poetry run tigris-mcap-benchmark /path/to/file.mcap my-bucket --prefix test/run1`

## S3 Defaults
- Edit `src/tigris_mcap_benchmark/config.py` to set:
  - `DEFAULT_BUCKET` – your default bucket name
  - `DEFAULT_ENDPOINT_URL` – S3-compatible endpoint URL (optional)
  - `DEFAULT_USE_PATH_STYLE` – toggle path-style addressing
