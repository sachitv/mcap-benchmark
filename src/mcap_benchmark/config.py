"""
Centralized configuration for S3 bucket and endpoint defaults.

Edit these constants to set project defaults. CLI flags and environment
variables will override these values at runtime.
"""

# Default S3 bucket name. You can override via CLI `--bucket` or env `S3_BUCKET`.
DEFAULT_BUCKET: str = ""

# Optional S3-compatible endpoint URL (e.g., MinIO, Cloudflare R2, etc.)
# Example: "http://localhost:9000" or "https://accountid.r2.cloudflarestorage.com"
DEFAULT_ENDPOINT_URL: str | None = None

# Whether to use path-style addressing ("https://endpoint/bucket/key")
# Some S3-compatible services require this.
DEFAULT_USE_PATH_STYLE: bool = False

