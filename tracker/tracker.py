import requests
import datetime
from collections import defaultdict, deque
import os
import threading
from reactivex import from_iterable, operators as ops
from reactivex.scheduler import ThreadPoolScheduler
from reactivex.scheduler import NewThreadScheduler
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

GITHUB_API_URL = "https://api.github.com"
GITHUB_ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
REQUESTS_PER_MINUTE = 30
thread_pool_scheduler = ThreadPoolScheduler(max_workers=10)

new_repos_storage = defaultdict(set)
lang_stats_storage = defaultdict(lambda: defaultdict(int))
storage_lock = threading.Lock()

@dataclass
class GithubEvent:
    repo_fullname: str
    keyword: str
    found_date: datetime.datetime
    match_cnt: int
    langs: list

def get_authorization_headers():
    return {
        "Authorization": f"Bearer {GITHUB_ACCESS_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }

def fetch_github_api(url: str):
    response = requests.get(url, headers=get_authorization_headers())
    return response.json() if response.status_code == 200 else None

def get_languages(languages_url):
    languages_data = fetch_github_api(languages_url)
    return list(languages_data.keys()) if languages_data else []

def process_search_results(keyword, results):
    return from_iterable(
        GithubEvent(
            repo_fullname=item["repository"]["full_name"],
            keyword=keyword,
            found_date=datetime.datetime.now(),
            match_cnt=1,
            langs=get_languages(item["repository"]["languages_url"])
        ) for item in results.get("items", [])
    )

def filter_new_repos(src):
    def is_new_repo(event):
        with storage_lock:
            if event.repo_fullname not in new_repos_storage[event.keyword]:
                new_repos_storage[event.keyword].add(event.repo_fullname)
                return True
        return False
    return src.pipe(ops.filter(is_new_repo))

def aggregate_language_stats(src):
    def update_stats(event):
        with storage_lock:
            for lang in event.langs:
                lang_stats_storage[event.keyword][lang] += 1
            return event

    def emit_stats(event):
        with storage_lock:
            stats = [{'keyword': event.keyword, 'lang': lang, 'count': count}
                     for lang, count in lang_stats_storage[event.keyword].items()]
        return from_iterable(stats)

    return src.pipe(
        ops.do_action(update_stats),
        ops.flat_map(emit_stats)
    )

def track_keyword(keyword):
    url = f"{GITHUB_API_URL}/search/code?q={keyword}"
    results = fetch_github_api(url)
    if results:
        events_observable = process_search_results(keyword, results)
        # .pipe(ops.observe_on(NewThreadScheduler())) will make each iteration run in a separate thread
        new_repos_observable = filter_new_repos(events_observable).pipe(ops.observe_on(NewThreadScheduler()))
        # .pipe(ops.observe_on(NewThreadScheduler())) will make each iteration run in a separate thread
        lang_stats_observable = aggregate_language_stats(events_observable).pipe(ops.observe_on(NewThreadScheduler()))
        return new_repos_observable, lang_stats_observable
    return None, None

def main():
    keyword = "python"
    new_repos_observable, lang_stats_observable = track_keyword(keyword)

    if new_repos_observable and lang_stats_observable:
        new_repos_observable.subscribe(
            on_next=lambda event: print(f"New repo (Thread: {threading.current_thread().name}): {event.repo_fullname}"),
            on_error=lambda error: print(f"Error in new repos observable: {error}"),
            on_completed=lambda: print("New repos tracking completed")
        )
        lang_stats_observable.subscribe(
            on_next=lambda stat: print(f"Lang stats (Thread: {threading.current_thread().name}): {stat}"),
            on_error=lambda error: print(f"Error in lang stats observable: {error}"),
            on_completed=lambda: print("Language stats tracking completed")
        )

if __name__ == "__main__":
    main()
