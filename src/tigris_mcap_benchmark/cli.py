import argparse
import asyncio
import csv
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import boto3
from botocore.config import Config as BotoConfig
from mcap.reader import make_reader
from . import config as cfg
import subprocess
import threading


class _Progress:
    """Simple, thread-safe progress bar for upload bytes.

    Writes a single updating line to stderr to avoid interfering with stdout.
    """

    def __init__(self, total_bytes: int, total_files: int) -> None:
        self.total_bytes = max(0, int(total_bytes))
        self.total_files = max(0, int(total_files))
        self._uploaded_bytes = 0
        self._done_files = 0
        self._start = time.perf_counter()
        self._last_render = 0.0
        self._lock = threading.Lock()
        # Initial render
        self._render(force=True)

    def add_bytes(self, n: int) -> None:
        if n <= 0:
            return
        with self._lock:
            self._uploaded_bytes += n
            self._render()

    def file_done(self, size_bytes: int | None = None) -> None:
        with self._lock:
            self._done_files += 1
            if size_bytes:
                self._uploaded_bytes = min(self.total_bytes, self._uploaded_bytes)
            self._render(force=True)

    def _format_bytes(self, b: int) -> str:
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if b < 1024 or unit == "TB":
                return f"{b:.0f}{unit}" if unit == "B" else f"{b/1024:.1f}{unit}"
            b /= 1024
        return f"{b:.1f}TB"

    def _render(self, force: bool = False) -> None:
        now = time.perf_counter()
        if not force and (now - self._last_render) < 0.1:
            return
        self._last_render = now
        elapsed = max(1e-6, now - self._start)
        uploaded = min(self._uploaded_bytes, self.total_bytes) if self.total_bytes else self._uploaded_bytes
        pct = (uploaded / self.total_bytes) if self.total_bytes else 0.0
        bar_width = 30
        filled = int(pct * bar_width)
        bar = "#" * filled + "-" * (bar_width - filled)
        rate_bps = (uploaded / elapsed)
        rate_mbps = (rate_bps * 8) / 1_000_000
        msg = (
            f"\r[{bar}] {pct*100:6.2f}%  "
            f"{self._format_bytes(uploaded)}/{self._format_bytes(self.total_bytes)}  "
            f"files {self._done_files}/{self.total_files}  "
            f"{rate_mbps:6.2f} Mb/s"
        )
        try:
            sys.stderr.write(msg)
            sys.stderr.flush()
        except Exception:
            pass

    def finish(self) -> None:
        with self._lock:
            self._uploaded_bytes = max(self._uploaded_bytes, self.total_bytes)
            self._done_files = max(self._done_files, self.total_files)
            self._render(force=True)
            try:
                sys.stderr.write("\n")
                sys.stderr.flush()
            except Exception:
                pass


@dataclass
class ExtractedMessage:
    index: int
    channel_id: int
    topic: str
    log_time: int
    publish_time: int
    size_bytes: int
    file_path: Path
    s3_key: str


@dataclass
class UploadResult:
    index: int
    s3_key: str
    size_bytes: int
    duration_sec: float


def extract_messages(
    mcap_path: Path,
    out_dir: Path,
    key_prefix: str,
    start: int = 0,
    limit: Optional[int] = None,
) -> List[ExtractedMessage]:
    out_dir.mkdir(parents=True, exist_ok=True)

    extracted: List[ExtractedMessage] = []
    count = 0
    with mcap_path.open("rb") as f:
        reader = make_reader(f)
        for idx, (schema, channel, message) in enumerate(reader.iter_messages()):
            if idx < start:
                continue
            if limit is not None and count >= limit:
                break

            data = message.data
            size = len(data)
            # Compose a deterministic filename and S3 key
            topic_safe = (channel.topic or "unknown").strip("/").replace("/", "_") or "unknown"
            filename = f"{idx:08d}__ch{channel.id}__{topic_safe}__log{message.log_time}.bin"
            file_path = out_dir / filename

            with file_path.open("wb") as fp:
                fp.write(data)

            key = f"{key_prefix}/{filename}" if key_prefix else filename
            extracted.append(
                ExtractedMessage(
                    index=idx,
                    channel_id=channel.id,
                    topic=channel.topic,
                    log_time=message.log_time,
                    publish_time=message.publish_time,
                    size_bytes=size,
                    file_path=file_path,
                    s3_key=key,
                )
            )
            count += 1

    return extracted


def upload_files_sequential(
    bucket: str,
    extracted: List[ExtractedMessage],
    cleanup: bool = False,
    region: Optional[str] = None,
    profile: Optional[str] = None,
    endpoint_url: Optional[str] = None,
    use_path_style: bool = False,
    credentials: Optional[dict] = None,
) -> List[UploadResult]:
    # Configure session optionally with profile/region/endpoint/credentials
    session_kwargs = {}
    if profile:
        session_kwargs["profile_name"] = profile
    session = boto3.session.Session(**session_kwargs)
    boto_cfg = BotoConfig(s3={"addressing_style": "path" if use_path_style else "virtual"})
    client_kwargs = {"region_name": region, "config": boto_cfg, "endpoint_url": endpoint_url}
    if credentials:
        client_kwargs.update(credentials)
    s3 = session.client("s3", **{k: v for k, v in client_kwargs.items() if v is not None})

    total_bytes = sum(e.size_bytes for e in extracted)
    progress = _Progress(total_bytes=total_bytes, total_files=len(extracted))

    results: List[UploadResult] = []
    for item in extracted:
        with item.file_path.open("rb") as fp:
            start = time.perf_counter()
            # upload_fileobj handles multipart and retries under the hood
            s3.upload_fileobj(fp, bucket, item.s3_key, Callback=progress.add_bytes)
            duration = time.perf_counter() - start
        results.append(
            UploadResult(
                index=item.index, s3_key=item.s3_key, size_bytes=item.size_bytes, duration_sec=duration
            )
        )
        progress.file_done(item.size_bytes)
        if cleanup:
            try:
                item.file_path.unlink(missing_ok=True)
            except Exception:
                # Best-effort cleanup; continue on errors
                pass

    progress.finish()
    return results


async def upload_files_async(
    bucket: str,
    extracted: List[ExtractedMessage],
    concurrency: int = 8,
    cleanup: bool = False,
    region: Optional[str] = None,
    profile: Optional[str] = None,
    endpoint_url: Optional[str] = None,
    use_path_style: bool = False,
    credentials: Optional[dict] = None,
) -> List[UploadResult]:
    session_kwargs = {}
    if profile:
        session_kwargs["profile_name"] = profile
    session = boto3.session.Session(**session_kwargs)
    boto_cfg = BotoConfig(s3={"addressing_style": "path" if use_path_style else "virtual"})
    client_kwargs = {"region_name": region, "config": boto_cfg, "endpoint_url": endpoint_url}
    if credentials:
        client_kwargs.update(credentials)
    s3 = session.client("s3", **{k: v for k, v in client_kwargs.items() if v is not None})

    sem = asyncio.Semaphore(max(1, int(concurrency)))
    progress = _Progress(total_bytes=sum(e.size_bytes for e in extracted), total_files=len(extracted))
    results: List[UploadResult] = []
    lock = asyncio.Lock()

    async def do_one(item: ExtractedMessage) -> None:
        async with sem:
            start = time.perf_counter()
            # Offload blocking upload to a thread to enable asyncio concurrency
            await asyncio.to_thread(
                s3.upload_file, str(item.file_path), bucket, item.s3_key, Callback=progress.add_bytes
            )
            duration = time.perf_counter() - start

            if cleanup:
                try:
                    item.file_path.unlink(missing_ok=True)
                except Exception:
                    pass

            # Update progress for completed file
            progress.file_done(item.size_bytes)

            async with lock:
                results.append(
                    UploadResult(
                        index=item.index,
                        s3_key=item.s3_key,
                        size_bytes=item.size_bytes,
                        duration_sec=duration,
                    )
                )

    await asyncio.gather(*(do_one(item) for item in extracted))
    progress.finish()
    # Keep order stable by index
    results.sort(key=lambda r: r.index)
    return results


def write_csv(results: List[UploadResult], csv_path: Path) -> None:
    with csv_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["index", "s3_key", "size_bytes", "duration_sec", "throughput_mbps"])
        for r in results:
            mbps = (r.size_bytes * 8 / 1_000_000) / r.duration_sec if r.duration_sec > 0 else 0.0
            writer.writerow([r.index, r.s3_key, r.size_bytes, f"{r.duration_sec:.6f}", f"{mbps:.3f}"])


def summarize(results: List[UploadResult]) -> dict:
    total_bytes = sum(r.size_bytes for r in results)
    total_time = sum(r.duration_sec for r in results)
    count = len(results)
    avg_time = (total_time / count) if count else 0.0
    avg_size = (total_bytes / count) if count else 0
    avg_mbps = ((avg_size * 8) / 1_000_000) / avg_time if avg_time > 0 else 0.0
    return {
        "files": count,
        "total_bytes": total_bytes,
        "total_time_sec": total_time,
        "avg_time_sec": avg_time,
        "avg_size_bytes": int(avg_size),
        "avg_throughput_mbps": avg_mbps,
    }


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="tigris-mcap-benchmark",
        description="Extract messages from an MCAP file, upload each as a binary to S3, and benchmark upload timings.",
    )
    p.add_argument("mcap", type=Path, help="Path to the MCAP file to process")
    p.add_argument("bucket", nargs="?", type=str, help="Target S3 bucket name (or use --bucket/env/default)")
    p.add_argument(
        "--prefix",
        "-p",
        default="",
        help="Optional S3 key prefix (folder-like path) to place objects under",
    )
    p.add_argument(
        "--out-dir",
        "-o",
        type=Path,
        default=Path("./mcap_messages_out"),
        help="Directory to write extracted message binary files",
    )
    p.add_argument("--start", type=int, default=0, help="Start from message index (0-based)")
    p.add_argument("--limit", type=int, default=None, help="Limit number of messages to process")
    p.add_argument(
        "--region",
        default=None,
        help="AWS region for the S3 client (defaults to environment/config)",
    )
    p.add_argument(
        "--profile",
        default=None,
        help="AWS profile name to use for credentials (optional)",
    )
    p.add_argument(
        "--endpoint-url",
        default=None,
        help="Custom S3-compatible endpoint URL (e.g., http://localhost:9000)",
    )
    p.add_argument(
        "--path-style",
        action="store_true",
        help="Use path-style addressing (required by some S3-compatible services)",
    )
    p.add_argument(
        "--cleanup",
        action="store_true",
        help="Delete extracted files after successful upload",
    )
    p.add_argument(
        "--concurrency",
        "-c",
        type=int,
        default=1,
        help="Concurrent uploads via asyncio.to_thread (1 = sequential)",
    )
    p.add_argument(
        "--bucket",
        dest="bucket_flag",
        default=None,
        help="Bucket name override (alternative to positional arg)",
    )
    # 1Password integration (requires `op` CLI and signed-in session)
    p.add_argument(
        "--op-access",
        default=None,
        help="1Password reference for AWS access key, e.g. op://Vault/Item/AccessKeyId",
    )
    p.add_argument(
        "--op-secret",
        default=None,
        help="1Password reference for AWS secret, e.g. op://Vault/Item/SecretAccessKey",
    )
    p.add_argument(
        "--op-session-token",
        default=None,
        help="Optional 1Password reference for AWS session token, e.g. op://Vault/Item/SessionToken",
    )
    p.add_argument(
        "--csv",
        type=Path,
        default=None,
        help="Path to write a CSV of per-file upload results (defaults to <out-dir>/upload_results.csv)",
    )
    return p.parse_args(argv)


def _op_read(ref: str) -> Optional[str]:
    """Read a secret value from 1Password using `op read <ref>`.

    Returns None if `op` is not available or the read fails.
    """
    try:
        proc = subprocess.run(
            ["op", "read", ref],
            check=True,
            capture_output=True,
            text=True,
        )
        return proc.stdout.strip()
    except Exception:
        return None


def resolve_bucket(args: argparse.Namespace) -> Optional[str]:
    # Priority: --bucket flag > positional arg > env S3_BUCKET > config.DEFAULT_BUCKET
    return args.bucket_flag or args.bucket or os.getenv("S3_BUCKET") or (cfg.DEFAULT_BUCKET or None)


def resolve_endpoint(args: argparse.Namespace) -> Optional[str]:
    # Priority: --endpoint-url > env S3_ENDPOINT_URL > config.DEFAULT_ENDPOINT_URL
    return args.endpoint_url or os.getenv("S3_ENDPOINT_URL") or cfg.DEFAULT_ENDPOINT_URL


def resolve_credentials(args: argparse.Namespace) -> Optional[dict]:
    # 1) Try 1Password references if provided
    access = _op_read(args.op_access) if args.op_access else None
    secret = _op_read(args.op_secret) if args.op_secret else None
    token = _op_read(args.op_session_token) if args.op_session_token else None

    # 2) Fall back to environment variables if not provided via 1Password
    access = access or os.getenv("AWS_ACCESS_KEY_ID")
    secret = secret or os.getenv("AWS_SECRET_ACCESS_KEY")
    token = token or os.getenv("AWS_SESSION_TOKEN")

    if access and secret:
        creds = {"aws_access_key_id": access, "aws_secret_access_key": secret}
        if token:
            creds["aws_session_token"] = token
        return creds
    return None


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    mcap_path: Path = args.mcap
    bucket: Optional[str] = resolve_bucket(args)
    prefix: str = args.prefix.strip("/")
    out_dir: Path = args.out_dir

    if not mcap_path.exists():
        print(f"MCAP file not found: {mcap_path}", file=sys.stderr)
        return 2
    if not bucket:
        print(
            "Bucket name not provided. Pass positional <bucket>, use --bucket, set env S3_BUCKET, or edit config.DEFAULT_BUCKET.",
            file=sys.stderr,
        )
        return 2

    print(f"Extracting messages from: {mcap_path}", flush=True)
    extracted = extract_messages(
        mcap_path=mcap_path,
        out_dir=out_dir,
        key_prefix=prefix,
        start=args.start,
        limit=args.limit,
    )
    print(f"Extracted {len(extracted)} messages to: {out_dir}", flush=True)

    endpoint_url = resolve_endpoint(args)
    use_path_style = bool(args.path_style or cfg.DEFAULT_USE_PATH_STYLE)
    credentials = resolve_credentials(args)

    print(
        f"Uploading to s3://{bucket}/{prefix}" + ("/" if prefix else "") +
        (f" via {endpoint_url}" if endpoint_url else "") +
        (" (path-style)" if use_path_style else "")
    , flush=True)
    # Ensure all prior messages are flushed before starting progress output
    try:
        sys.stdout.flush()
        sys.stderr.flush()
    except Exception:
        pass
    if args.concurrency and int(args.concurrency) > 1:
        results = asyncio.run(
            upload_files_async(
                bucket=bucket,
                extracted=extracted,
                concurrency=int(args.concurrency),
                cleanup=args.cleanup,
                region=args.region,
                profile=args.profile,
                endpoint_url=endpoint_url,
                use_path_style=use_path_style,
                credentials=credentials,
            )
        )
    else:
        results = upload_files_sequential(
            bucket=bucket,
            extracted=extracted,
            cleanup=args.cleanup,
            region=args.region,
            profile=args.profile,
            endpoint_url=endpoint_url,
            use_path_style=use_path_style,
            credentials=credentials,
        )

    # Per-file summary lines
    for r in results:
        mbps = (r.size_bytes * 8 / 1_000_000) / r.duration_sec if r.duration_sec > 0 else 0.0
        print(
            f"uploaded index={r.index} key={r.s3_key} size={r.size_bytes}B time={r.duration_sec:.4f}s rate={mbps:.2f} Mb/s"
        )

    # Overall summary
    summary = summarize(results)
    print(
        "Summary: files={files} total_bytes={total_bytes} total_time={total_time_sec:.3f}s "
        "avg_time={avg_time_sec:.4f}s avg_size={avg_size_bytes}B avg_rate={avg_throughput_mbps:.2f} Mb/s".format(
            **summary
        )
    )

    # CSV output
    csv_path = args.csv or (out_dir / "upload_results.csv")
    write_csv(results, csv_path)
    print(f"Wrote CSV results to: {csv_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
