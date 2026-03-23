import os
import csv
import time
import threading
import requests
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

load_dotenv()

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")
if not GITHUB_TOKEN:
    raise EnvironmentError("GITHUB_TOKEN environment variable not set")

HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json",
}

OUTPUT_FILE = "repositories.csv"
FIELDNAMES = [
    "full_name", "stars", "age_years", "releases_count",
    "primary_language", "forks_count", "open_issues_count",
    "created_at", "updated_at",
]

MAX_WORKERS = 10
_print_lock = threading.Lock()
_rate_limit_lock = threading.Lock()

_local = threading.local()


def get_session():
    if not hasattr(_local, "session"):
        s = requests.Session()
        s.headers.update(HEADERS)
        _local.session = s
    return _local.session


def request_with_retry(url, params=None):
    session = get_session()
    while True:
        response = session.get(url, params=params)
        if response.status_code in (403, 429):
            with _rate_limit_lock:
                reset_time = response.headers.get("X-RateLimit-Reset")
                if reset_time:
                    wait = max(int(reset_time) - int(time.time()), 1)
                else:
                    wait = int(response.headers.get("Retry-After", 60))
                with _print_lock:
                    print(f"  Rate limit hit. Waiting {wait}s...")
                time.sleep(wait)
            continue
        return response


def get_releases_count(full_name):
    url = f"https://api.github.com/repos/{full_name}/releases"
    response = request_with_retry(url, params={"per_page": 1})
    if response.status_code != 200:
        return 0
    link_header = response.headers.get("Link", "")
    if 'rel="last"' in link_header:
        for part in link_header.split(","):
            if 'rel="last"' in part:
                url_part = part.split(";")[0].strip().strip("<>")
                return int(url_part.split("page=")[-1])
    return len(response.json())


def calc_age_years(created_at_str):
    created_at = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
    now = datetime.now(timezone.utc)
    return round((now - created_at).days / 365.25, 2)


def process_repo(repo, global_index, total):
    full_name = repo["full_name"]
    releases = get_releases_count(full_name)
    row = {
        "full_name": full_name,
        "stars": repo["stargazers_count"],
        "age_years": calc_age_years(repo["created_at"]),
        "releases_count": releases,
        "primary_language": repo.get("language") or "",
        "forks_count": repo["forks_count"],
        "open_issues_count": repo["open_issues_count"],
        "created_at": repo["created_at"],
        "updated_at": repo["updated_at"],
    }
    with _print_lock:
        print(f"[{global_index}/{total}] {full_name} — {row['stars']} stars, {releases} releases")
    return global_index, row


def collect_repositories():
    per_page = 100
    total_pages = 10
    total = total_pages * per_page

    print("Collecting top-1000 Java repositories from GitHub...")

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, delimiter=";")
        writer.writeheader()

        total_saved = 0
        global_index = 0

        for page in range(1, total_pages + 1):
            print(f"\nFetching search page {page}/{total_pages}...")
            response = request_with_retry(
                "https://api.github.com/search/repositories",
                params={
                    "q": "language:java",
                    "sort": "stars",
                    "order": "desc",
                    "per_page": per_page,
                    "page": page,
                },
            )
            if response.status_code != 200:
                print(f"  Error on page {page}: {response.status_code} {response.text}")
                break
            items = response.json().get("items", [])
            if not items:
                break

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {
                    executor.submit(process_repo, repo, global_index + i + 1, total): i
                    for i, repo in enumerate(items)
                }
                global_index += len(items)

                results = [None] * len(items)
                for future in as_completed(futures):
                    original_i = futures[future]
                    _, row = future.result()
                    results[original_i] = row

            writer.writerows(results)
            f.flush()
            total_saved += len(results)
            print(f"  Batch written: {len(results)} repos (total so far: {total_saved})")

    print(f"\nDone. {total_saved} repositories saved to {OUTPUT_FILE}.")


if __name__ == "__main__":
    collect_repositories()
