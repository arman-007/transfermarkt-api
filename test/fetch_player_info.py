#!/usr/bin/env python3
"""
Fetch first player search result from a local Transfermarkt FastAPI endpoint
and store aggregated results in output/player_info.json.

Usage:
python fetch_players.py --csv files/players.csv \
    --output output/player_info.json \
    --base-url http://localhost:8000 \
    --resume
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any

import requests
from requests import Session
from urllib.parse import quote
import random

DEFAULT_BASE_URL = "http://localhost:8000"
SEARCH_PATH_TMPL = "/players/search/{name}"

@dataclass
class PlayerFetchRecord:
    query: str
    success: bool
    status_code: Optional[int]
    duration_sec: float
    error: Optional[str]
    first_result: Optional[Dict[str, Any]]  # as returned by your API (or None)

def setup_logging(log_path: Path) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    # Console
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # File
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

def load_existing(output_path: Path) -> Dict[str, Any]:
    if output_path.exists():
        with open(output_path, "r", encoding="utf-8") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                logging.warning("Existing output JSON is invalid; starting fresh.")
                return {}
    return {}

def save_json_atomic(data: Dict[str, Any], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_suffix(".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp_path.replace(out_path)

def read_names_from_csv(csv_path: Path) -> List[str]:
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        return [ln.strip() for ln in f if ln.strip()]
    
def fetch_first_result(session: Session, base_url: str, name: str, page_number: int = 1,
                    max_retries: int = 3, timeout: int = 20) -> PlayerFetchRecord:
    """
    Calls /players/search/{name}?page_number=1 and returns the first result if present.
    Retries with exponential backoff on 5xx/429/connection errors.
    """
    encoded = quote(name)
    url = f"{base_url}{SEARCH_PATH_TMPL.format(name=encoded)}"
    params = {"page_number": page_number}
    headers = {"accept": "application/json"}

    start = time.perf_counter()
    attempt = 0
    last_exc: Optional[Exception] = None
    status_code: Optional[int] = None

    while attempt < max_retries:
        attempt += 1
        try:
            logging.info(f"Fetching '{name}' (attempt {attempt}/{max_retries}) → {url}")
            resp = session.get(url, params=params, headers=headers, timeout=timeout)
            status_code = resp.status_code

            # Handle rate-limit or server hiccups as retryable
            if status_code in (429, 502, 503, 504):
                msg = f"Retryable status {status_code} for '{name}'"
                logging.warning(msg)
                raise requests.HTTPError(msg)

            resp.raise_for_status()
            data = resp.json()

            results = data.get("results", [])
            first = results[0] if results else None

            duration = time.perf_counter() - start
            if first is None:
                return PlayerFetchRecord(
                    query=name,
                    success=False,
                    status_code=status_code,
                    duration_sec=duration,
                    error="No results",
                    first_result=None,
                )

            return PlayerFetchRecord(
                query=name,
                success=True,
                status_code=status_code,
                duration_sec=duration,
                error=None,
                first_result=first,
            )

        except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as e:
            last_exc = e
            # Exponential backoff with jitter
            sleep_s = min(2 ** attempt + random.uniform(0, 0.5), 8.0)
            logging.warning(f"Error fetching '{name}': {e}. Backing off {sleep_s:.1f}s...")
            time.sleep(sleep_s)
        except Exception as e:
            last_exc = e
            break

    duration = time.perf_counter() - start
    return PlayerFetchRecord(
        query=name,
        success=False,
        status_code=status_code,
        duration_sec=duration,
        error=str(last_exc) if last_exc else "Unknown error",
        first_result=None,
    )

def main():
    parser = argparse.ArgumentParser(description="Fetch player info from local API.")
    parser.add_argument("--csv", required=True, help="Path to CSV with player names (default column 'name' or first col).")
    parser.add_argument("--output", default="output/player_info.json", help="Path to aggregated JSON output.")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Base URL of your FastAPI app.")
    parser.add_argument("--resume", action="store_true", help="Resume: skip players already saved successfully.")
    parser.add_argument("--sleep", type=float, default=0.0, help="Optional sleep seconds between requests.")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    output_path = Path(args.output)
    log_path = output_path.with_suffix(".log")

    setup_logging(log_path)

    if not csv_path.exists():
        logging.error(f"CSV not found: {csv_path}")
        sys.exit(1)

    names = read_names_from_csv(csv_path)
    if not names:
        logging.error("No player names found in CSV.")
        sys.exit(1)

    logging.info(f"Loaded {len(names)} names from {csv_path}")

    # Load existing results to support resume
    existing = load_existing(output_path)
    existing_items = existing.get("items", [])
    processed_success_names = {
        item.get("query")
        for item in existing_items
        if item.get("success") is True and item.get("firstResult")  # legacy key check
    } | {
        item.get("query")
        for item in existing_items
        if item.get("success") is True and item.get("first_result")
    }

    items: List[Dict[str, Any]] = existing_items if args.resume else []

    session = requests.Session()

    total_start = time.perf_counter()
    success_count = 0
    fail_count = 0

    for name in names:
        if args.resume and name in processed_success_names:
            logging.info(f"Skipping (resume): '{name}' already fetched successfully.")
            continue

        rec = fetch_first_result(session, args.base_url, name)
        # Log outcome
        if rec.success:
            success_count += 1
            logging.info(f"OK: '{name}' in {rec.duration_sec:.3f}s → {rec.first_result.get('name') if rec.first_result else 'N/A'}")
        else:
            fail_count += 1
            logging.error(f"FAIL: '{name}' in {rec.duration_sec:.3f}s → {rec.error}")

        # Append to items (store clean structure)
        items.append({
            "query": rec.query,
            "success": rec.success,
            "statusCode": rec.status_code,
            "durationSec": round(rec.duration_sec, 3),
            "first_result": rec.first_result,  # keep API shape as-is
            "error": rec.error,
        })

        # Optional pacing
        if args.sleep > 0:
            time.sleep(args.sleep)

        # Periodically flush to disk to be safe
        if len(items) % 20 == 0:
            payload = {
                "generatedAt": datetime.now(timezone.utc).isoformat(),
                "baseUrl": args.base_url,
                "totalPlayers": len(names),
                "successCount": success_count,
                "failCount": fail_count,
                "items": items,
            }
            save_json_atomic(payload, output_path)
            logging.info(f"Checkpoint saved → {output_path}")

    total_duration = time.perf_counter() - total_start

    # Final write
    payload = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "baseUrl": args.base_url,
        "totalPlayers": len(names),
        "successCount": success_count,
        "failCount": fail_count,
        "totalDurationSec": round(total_duration, 3),
        "items": items,
    }
    save_json_atomic(payload, output_path)

    logging.info(f"Done. Success: {success_count}, Fail: {fail_count}, Total: {len(names)}, Elapsed: {total_duration:.3f}s")
    logging.info(f"Wrote JSON → {output_path}")
    logging.info(f"Full log → {log_path}")

if __name__ == "__main__":
    main()
