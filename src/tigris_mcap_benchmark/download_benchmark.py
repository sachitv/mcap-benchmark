import argparse
import asyncio
import csv
import os
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import boto3
from botocore.config import Config as BotoConfig

from . import config as cfg


@dataclass
class DownloadResult:
    key: str
    size_bytes: int
    first_byte_sec: float
    total_time_sec: float
    path: Path


def resolve_bucket(args: argparse.Namespace) -> Optional[str]:
    # Priority: --bucket flag > positional arg > env S3_BUCKET > config.DEFAULT_BUCKET
    return args.bucket_flag or args.bucket or os.getenv("S3_BUCKET") or (cfg.DEFAULT_BUCKET or None)


def resolve_endpoint(args: argparse.Namespace) -> Optional[str]:
    # Priority: --endpoint-url > env S3_ENDPOINT_URL > config.DEFAULT_ENDPOINT_URL
    return args.endpoint_url or os.getenv("S3_ENDPOINT_URL") or cfg.DEFAULT_ENDPOINT_URL


def resolve_credentials(args: argparse.Namespace) -> Optional[dict]:
    # Read from environment variables (compatible with the upload CLI)
    access = os.getenv("AWS_ACCESS_KEY_ID")
    secret = os.getenv("AWS_SECRET_ACCESS_KEY")
    token = os.getenv("AWS_SESSION_TOKEN")

    if access and secret:
        creds = {"aws_access_key_id": access, "aws_secret_access_key": secret}
        if token:
            creds["aws_session_token"] = token
        return creds
    return None


def make_s3_client(
    region: Optional[str],
    profile: Optional[str],
    endpoint_url: Optional[str],
    use_path_style: bool,
    credentials: Optional[dict],
):
    session_kwargs = {}
    if profile:
        session_kwargs["profile_name"] = profile
    session = boto3.session.Session(**session_kwargs)
    boto_cfg = BotoConfig(s3={"addressing_style": "path" if use_path_style else "virtual"})
    client_kwargs = {"region_name": region, "config": boto_cfg, "endpoint_url": endpoint_url}
    if credentials:
        client_kwargs.update(credentials)
    return session.client("s3", **{k: v for k, v in client_kwargs.items() if v is not None})


def list_all_objects(s3, bucket: str, prefix: str = "") -> Iterable[Tuple[str, int]]:
    """Yield (key, size_bytes) for every object under the prefix."""
    paginator = s3.get_paginator("list_objects_v2")
    kwargs = {"Bucket": bucket}
    if prefix:
        kwargs["Prefix"] = prefix
    for page in paginator.paginate(**kwargs):
        for obj in page.get("Contents", []) or []:
            yield obj["Key"], int(obj.get("Size", 0))


def download_one(
    s3,
    bucket: str,
    key: str,
    dest_dir: Path,
    chunk_size: int = 64 * 1024,
) -> DownloadResult:
    dest_path = dest_dir / key
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    start = time.perf_counter()
    resp = s3.get_object(Bucket=bucket, Key=key)
    body = resp["Body"]
    size_hdr = int(resp.get("ContentLength") or 0)

    first_byte_sec: Optional[float] = None
    total_bytes = 0

    with dest_path.open("wb") as out:
        for chunk in body.iter_chunks(chunk_size=chunk_size):
            if not chunk:
                continue
            if first_byte_sec is None:
                first_byte_sec = time.perf_counter() - start
            out.write(chunk)
            total_bytes += len(chunk)

    total_time_sec = time.perf_counter() - start
    if first_byte_sec is None:
        first_byte_sec = total_time_sec  # empty object edge case

    size_bytes = size_hdr or total_bytes
    return DownloadResult(
        key=key, size_bytes=size_bytes, first_byte_sec=first_byte_sec, total_time_sec=total_time_sec, path=dest_path
    )


def write_csv(results: List[DownloadResult], csv_path: Path) -> None:
    with csv_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["key", "size_bytes", "first_byte_sec", "total_time_sec", "throughput_mbps", "path"])
        for r in results:
            mbps = (r.size_bytes * 8 / 1_000_000) / r.total_time_sec if r.total_time_sec > 0 else 0.0
            w.writerow([r.key, r.size_bytes, f"{r.first_byte_sec:.6f}", f"{r.total_time_sec:.6f}", f"{mbps:.3f}", str(r.path)])


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="tigris-mcap-download-benchmark",
        description="Download all objects from an S3 bucket (optional prefix) to a temporary directory and benchmark first-byte and total download time.",
    )
    p.add_argument("bucket", nargs="?", help="S3 bucket name (or use --bucket/env/config)")
    p.add_argument("--bucket", dest="bucket_flag", default=None, help="Bucket name override")
    p.add_argument("--prefix", default="", help="Optional key prefix to filter objects")
    p.add_argument("--limit", type=int, default=None, help="Limit number of objects to download")
    p.add_argument("--region", default=None, help="AWS region for the S3 client")
    p.add_argument("--profile", default=None, help="AWS profile name to use")
    p.add_argument("--endpoint-url", default=None, help="Custom S3-compatible endpoint URL")
    p.add_argument("--path-style", action="store_true", help="Use path-style addressing")
    p.add_argument(
        "--tmp-dir",
        type=Path,
        default=None,
        help="Directory to download into (defaults to a created temporary directory)",
    )
    p.add_argument("--csv", type=Path, default=None, help="Optional path to write CSV results")
    p.add_argument(
        "--concurrency",
        "-c",
        type=int,
        default=1,
        help="Concurrent downloads via asyncio.to_thread (1 = sequential)",
    )
    return p.parse_args(argv)


async def download_all_async(
    s3,
    bucket: str,
    objects: List[Tuple[str, int]],
    dest_dir: Path,
    concurrency: int = 8,
) -> List[DownloadResult]:
    sem = asyncio.Semaphore(max(1, int(concurrency)))
    results: List[Tuple[int, DownloadResult]] = []
    lock = asyncio.Lock()

    total = len(objects)

    async def do_one(idx: int, key: str, size: int) -> None:
        async with sem:
            print(f"[{idx}/{total}] GET {key} ({size}B)...", flush=True)
            r = await asyncio.to_thread(download_one, s3, bucket, key, dest_dir)
            mbps = (r.size_bytes * 8 / 1_000_000) / r.total_time_sec if r.total_time_sec > 0 else 0.0
            print(
                f"    first_byte={r.first_byte_sec:.4f}s total={r.total_time_sec:.4f}s size={r.size_bytes}B rate={mbps:.2f} Mb/s -> {r.path}",
                flush=True,
            )
            async with lock:
                results.append((idx, r))

    await asyncio.gather(
        *(do_one(i, key, size) for i, (key, size) in enumerate(objects, start=1))
    )

    # Preserve original order
    results.sort(key=lambda t: t[0])
    return [r for _, r in results]


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    bucket = resolve_bucket(args)
    if not bucket:
        print(
            "Bucket name not provided. Pass positional <bucket>, use --bucket, set env S3_BUCKET, or edit config.DEFAULT_BUCKET.",
            file=sys.stderr,
        )
        return 2

    endpoint_url = resolve_endpoint(args)
    use_path_style = bool(args.path_style or cfg.DEFAULT_USE_PATH_STYLE)
    credentials = resolve_credentials(args)

    s3 = make_s3_client(
        region=args.region,
        profile=args.profile,
        endpoint_url=endpoint_url,
        use_path_style=use_path_style,
        credentials=credentials,
    )

    # Prepare destination directory
    if args.tmp_dir:
        dest_dir = args.tmp_dir
        dest_dir.mkdir(parents=True, exist_ok=True)
        tmp_info = f"{dest_dir}"
    else:
        dest_dir = Path(tempfile.mkdtemp(prefix="s3_download_"))
        tmp_info = f"{dest_dir} (created)"

    print(
        f"Downloading from s3://{bucket}/{args.prefix}" + ("/" if args.prefix else "") +
        (f" via {endpoint_url}" if endpoint_url else "") +
        (" (path-style)" if use_path_style else "") +
        f" to {tmp_info}",
        flush=True,
    )

    # Enumerate objects
    objects = list(list_all_objects(s3, bucket, args.prefix))
    if args.limit is not None:
        objects = objects[: max(0, int(args.limit))]
    if not objects:
        print("No objects found to download.")
        return 0

    # Download (optionally concurrent) and measure timings
    if args.concurrency and int(args.concurrency) > 1:
        results: List[DownloadResult] = asyncio.run(
            download_all_async(
                s3=s3,
                bucket=bucket,
                objects=objects,
                dest_dir=dest_dir,
                concurrency=int(args.concurrency),
            )
        )
    else:
        results = []
        total = len(objects)
        for i, (key, size) in enumerate(objects, start=1):
            print(f"[{i}/{total}] GET {key} ({size}B)...", flush=True)
            r = download_one(s3, bucket, key, dest_dir)
            mbps = (r.size_bytes * 8 / 1_000_000) / r.total_time_sec if r.total_time_sec > 0 else 0.0
            print(
                f"    first_byte={r.first_byte_sec:.4f}s total={r.total_time_sec:.4f}s size={r.size_bytes}B rate={mbps:.2f} Mb/s -> {r.path}",
                flush=True,
            )
            results.append(r)

    # Summary
    total_bytes = sum(r.size_bytes for r in results)
    avg_first_byte = sum(r.first_byte_sec for r in results) / len(results)
    total_time = sum(r.total_time_sec for r in results)
    avg_total_time = total_time / len(results)
    avg_size = total_bytes / len(results)
    avg_mbps = ((avg_size * 8) / 1_000_000) / avg_total_time if avg_total_time > 0 else 0.0

    print(
        "Summary: files={files} total_bytes={total_bytes} total_time={total_time:.3f}s "
        "avg_first_byte={avg_first_byte:.4f}s avg_total={avg_total:.4f}s avg_size={avg_size:.0f}B avg_rate={avg_mbps:.2f} Mb/s".format(
            files=len(results),
            total_bytes=total_bytes,
            total_time=total_time,
            avg_first_byte=avg_first_byte,
            avg_total=avg_total_time,
            avg_size=avg_size,
            avg_mbps=avg_mbps,
        )
    )

    # Optional CSV
    if args.csv:
        write_csv(results, args.csv)
        print(f"Wrote CSV results to: {args.csv}")

    # Print the destination directory for convenience
    print(f"Downloaded files under: {dest_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
