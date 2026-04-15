from __future__ import annotations

import requests
import sqlite3
import time
import hashlib
import json

from dataclasses import dataclass
from pathlib import Path
from typing import Any
from bs4 import BeautifulSoup

AI_ML_KEYWORDS = ["machine learning", "ml", "artificial intelligence", "ai ", "genai", "llm", "large language model", "foundation model",
                    "deep learning", "applied scientist", "scientist", "research engineer", "data scientist", "nlp", "computer vision",
                    "recommendation", "ranking", "autonomy",
]
LOCATION_KEYWORDS = ["us", "united states", "usa", "new york", "san francisco", "seattle",
                      "austin", "boston", "washington", "denver", "chicago", "atlanta", "california",
]
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*; q=0.01",
}
TIMEOUT = 20

@dataclass(frozen=True)
class Job:
    company: str
    title: str
    url: str
    location: str = ""
    posted_at: str = ""
    source: str = ""

    def fingerprint(self) -> str:
        key = f"{self.company}|{self.title}|{self.url}"
        return hashlib.sha256(key.encode("utf-8")).hexdigest()


def fetch_json(url: str, params: dict[str, Any] | None = None) -> Any:
    r = requests.get(url, params=params, headers=HEADERS, timeout=TIMEOUT)

    print("\n--- DEBUG ---")
    print("STATUS:", r.status_code)
    print("FINAL URL:", r.url)
    print("CONTENT-TYPE:", r.headers.get("content-type"))
    print("BODY START:", repr(r.text[:300]))
    print("-------------\n")

    r.raise_for_status()
    return r.json()


def is_ai_ml_job(job: dict[str, Any]) -> bool:
    text_parts = [
        str(job.get("title", "")),
        str(job.get("basic_qualifications", "")),
        str(job.get("description_short", "")),
        str(job.get("job_family", "")),
        str(job.get("job_category", "")),
    ]
    text = " ".join(text_parts).lower()
    return any(k in text for k in AI_ML_KEYWORDS)


def is_us_job(job: dict) -> bool:
    location = str(job.get("location", "")).lower()
    normalized = str(job.get("normalized_location", "")).lower()

    text = f"{location} {normalized}"

    return any(k in text for k in LOCATION_KEYWORDS)


def is_ai_in_us_job(job: dict) -> bool:
    return is_ai_ml_job(job) and is_us_job(job)
    # return True # For testing purposes, we can disable filtering to see all jobs


def first_nonempty(item: dict[str, Any], keys: list[str]) -> str:
    for key in keys:
        val = item.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
        if val is not None and not isinstance(val, (dict, list)):
            text = str(val).strip()
            if text:
                return text
    return ""


def fetch_html(url: str, params: dict[str, Any] | None = None) -> BeautifulSoup:
    r = requests.get(url, params=params, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")


def normalize_location(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        parts = []
        for key in ("city", "state", "region", "country", "name"):
            v = value.get(key)
            if v:
                parts.append(str(v).strip())
        return ", ".join(parts) if parts else json.dumps(value, ensure_ascii=False)
    if isinstance(value, list):
        return ", ".join(str(x).strip() for x in value if x)
    return str(value).strip()


def already_seen(conn: sqlite3.Connection, fp: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM seen_jobs WHERE fingerprint = ?",
        (fp,),
    ).fetchone()
    return row is not None


def mark_seen(conn: sqlite3.Connection, job: Job) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO seen_jobs 
        (fingerprint, company, title, url, location, first_seen_ts)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (job.fingerprint(), job.company, job.title, job.url, job.location, int(time.time()),),
    )
    conn.commit()


def init_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS seen_jobs (
            fingerprint TEXT PRIMARY KEY,
            company TEXT NOT NULL,
            title TEXT NOT NULL,
            url TEXT NOT NULL,
            location TEXT,
            first_seen_ts INTEGER NOT NULL
        )
        """
    )
    conn.commit()
    return conn