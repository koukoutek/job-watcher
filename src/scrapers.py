from __future__ import annotations

from typing import Any
from urllib.parse import urljoin

from helpers import is_ai_in_us_job, _first_nonempty, fetch_html, Job, fetch_json, normalize_location


def scrape_greenhouse(company: str, board_slug: str) -> list[Job]:
    api = f"https://boards-api.greenhouse.io/v1/boards/{board_slug}/jobs?content=true"
    data = fetch_json(api)

    jobs: list[Job] = []
    for item in data.get("jobs", []):

        if not is_ai_in_us_job(item):
            continue

        title = (item.get("title") or "").strip()
        url = item.get("absolute_url") or ""
        location = ""
        offices = item.get("offices") or []
        if offices:
            location = offices[0].get("name", "") or ""
        if title and url:
            jobs.append(
                Job(
                    company=company,
                    title=title,
                    url=url,
                    location = normalize_location(location),
                    posted_at=item.get("updated_at", "") or item.get("first_published_at", ""),
                    source="greenhouse",
                )
            )
    return jobs


def scrape_lever(company: str, company_slug: str) -> list[Job]:
    api = f"https://api.lever.co/v0/postings/{company_slug}?mode=json"
    data = fetch_json(api)

    jobs: list[Job] = []
    if not isinstance(data, list):
        return jobs

    for item in data:
        
        if not is_ai_in_us_job(item):
            continue

        title = (item.get("text") or "").strip()
        url = item.get("hostedUrl") or ""
        cats = item.get("categories") or {}
        location = (cats.get("location") or "").strip()

        if title and url:
            jobs.append(
                Job(
                    company=company,
                    title=title,
                    url=url,
                    location = normalize_location(location),
                    posted_at=str(item.get("createdAt", "")),
                    source="lever",
                )
            )
    return jobs


def scrape_amazon_json(company: str, spec: dict[str, Any]) -> list[Job]:
    """Scrape Amazon's careers JSON search endpoint.

    Expected config keys:
      - url: base endpoint, e.g. https://www.amazon.jobs/en/search
      - params: dict of query params, usually with result_limit and offset
      - item_url_prefix (optional): defaults to https://www.amazon.jobs
      - job_list_key (optional): defaults to 'jobs'
      - title_fields (optional): ordered list of candidate title fields
      - location_fields (optional): ordered list of candidate location fields
      - pagination:
          - limit_param (default result_limit)
          - offset_param (default offset)
          - start_offset (default 0)
          - limit (default from params[result_limit] or 50)
          - max_pages (optional)
          - stop_when_empty (default true)
    """
    url = spec["url"]
    base_params = dict(spec.get("params") or {})
    base_params["base_query"] = spec.get("base_query", "machine learning")
    item_url_prefix = spec.get("item_url_prefix", "https://www.amazon.jobs")
    job_list_key = spec.get("job_list_key", "jobs")
    title_fields = spec.get(
        "title_fields",
        ["title", "job_title", "name", "jobTitle", "display_title"],
    )
    location_fields = spec.get(
        "location_fields",
        ["location", "normalized_location", "city", "display_location"],
    )

    pagination = spec.get("pagination") or {}
    limit_param = pagination.get("limit_param", "result_limit")
    offset_param = pagination.get("offset_param", "offset")
    start_offset = int(pagination.get("start_offset", base_params.get(offset_param, 0) or 0))
    limit = int(pagination.get("limit", base_params.get(limit_param, 50) or 50))
    max_pages = pagination.get("max_pages")
    stop_when_empty = bool(pagination.get("stop_when_empty", True))

    jobs: list[Job] = []
    seen_urls: set[str] = set()
    page = 0
    offset = start_offset

    while True:
        params = dict(base_params)
        params[limit_param] = limit
        params[offset_param] = offset

        data = fetch_json(url, params=params)
        records = data.get(job_list_key, []) if isinstance(data, dict) else []
        if not isinstance(records, list):
            break

        if not records and stop_when_empty:
            break

        for item in records:
            if not isinstance(item, dict):
                continue

            if not is_ai_in_us_job(item):
                continue

            title = _first_nonempty(item, title_fields)
            location = _first_nonempty(item, location_fields)
            job_path = item.get("job_path") or item.get("path") or item.get("url") or ""

            if not title:
                # If the title field is nested or named differently, skip rather than inventing data.
                continue

            if isinstance(job_path, str) and job_path.startswith("http"):
                job_url = job_path
            else:
                job_url = urljoin(item_url_prefix, str(job_path)) if job_path else url

            if job_url in seen_urls:
                continue
            seen_urls.add(job_url)

            jobs.append(
                Job(
                    company=company,
                    title=title,
                    url=job_url,
                    location=normalize_location(location),
                    posted_at=str(item.get("posted_at", "") or item.get("created_at", "") or ""),
                    source="amazon_json",
                )
            )

        page += 1
        if max_pages is not None and page >= int(max_pages):
            break

        if len(records) < limit and stop_when_empty:
            break

        offset += limit

    return jobs


def scrape_zipline_json(company: str, spec: dict) -> list[Job]:
    url = spec["url"]

    data = fetch_json(url)

    jobs = []

    # You need to inspect structure, but typically:
    # data will be a list or contain a list
    records = data if isinstance(data, list) else data.get("jobs", [])

    for item in records:
        title = item.get("title") or item.get("name", "")
        location = item.get("location", "")
        path = item.get("slug") or item.get("id") or ""

        if not title:
            continue

        if not is_ai_in_us_job(item):
            continue

        job_url = f"https://www.zipline.com/careers/job/{path}"

        jobs.append(
            Job(
                company=company,
                title=title,
                url=job_url,
                location=normalize_location(location),
                source="zipline_json",
            )
        )

    return jobs


def scrape_generic_html(
    company: str,
    url: str,
    item_selector: str,
    title_selector: str,
    link_selector: str,
    location_selector: str = "",
) -> list[Job]:
    soup = fetch_html(url)
    jobs: list[Job] = []

    for item in soup.select(item_selector):
        title_el = item.select_one(title_selector)
        link_el = item.select_one(link_selector)

        if not title_el or not link_el:
            continue

        title = title_el.get_text(" ", strip=True)
        href = link_el.get("href") or ""
        job_url = urljoin(url, href)

        location = ""
        if location_selector:
            loc_el = item.select_one(location_selector)
            if loc_el:
                location = loc_el.get_text(" ", strip=True)

        if title and job_url:
            jobs.append(
                Job(
                    company=company,
                    title=title,
                    url=job_url,
                    location=normalize_location(location),
                    source="html",
                )
            )

    return jobs