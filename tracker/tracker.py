import requests
import datetime
from collections import defaultdict
from dotenv import load_dotenv
import os
from reactivex import Observable
from reactivex.scheduler import ThreadPoolScheduler
import reactivex.operators as ops
from dataclasses import dataclass
from typing import Set, Dict, List
from rx import from_iterable

load_dotenv()

GITHUB_API_URL = "https://api.github.com"
GITHUB_ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
REQUESTS_PER_MINUTE = 30

project_data = defaultdict(lambda: defaultdict(int))
language_stats = defaultdict(int)

@dataclass
class GithubEvent:
    repo_fullname: str
    keyword: str
    found_date: datetime.datetime
    match_cnt: int
    langs: List[str]

def get_authorization_headers():
    return {
        "Authorization": f"Bearer {GITHUB_ACCESS_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }

def fetch_github_api(url: str):
    response = requests.get(url, headers=get_authorization_headers())
    return response.json() if response.status_code == 200 else None

def process_search_results(keyword: str, results: dict):
    return from_iterable((
        GithubEvent(
            repo_fullname=item["repository"]["full_name"],
            keyword=keyword,
            found_date=datetime.datetime.now(),
            match_cnt=project_data[keyword][item["repository"]["full_name"]],
            langs=[fetch_github_api(item["repository"]["languages_url"])]
        ) for item in results["items"]
    ))

def track_keyword(keyword: str):
    url = f"{GITHUB_API_URL}/search/code?q={keyword}"
    results = fetch_github_api(url)
    if results:
        events_observable = process_search_results(keyword, results)
        return filter_and_aggregate(events_observable, keyword)
    return None

def filter_and_aggregate(observable: Observable, keyword: str):
    new_repos_storage = defaultdict(set)
    lang_stats_storage = defaultdict(lambda: defaultdict(int))

    new_repos_observable = observable.pipe(
        filter_new_repos(new_repos_storage),
        ops.subscribe_on(ThreadPoolScheduler())
    )

    lang_stats_observable = observable.pipe(
        aggregate_language_stats(lang_stats_storage),
        ops.subscribe_on(ThreadPoolScheduler())
    )

    return new_repos_observable, lang_stats_observable

def filter_new_repos(storage):
    def filter_func(event):
        return event.repo_fullname not in storage[event.keyword]

    def update_storage(event):
        storage[event.keyword].add(event.repo_fullname)

    return lambda src: src.pipe(
        ops.filter(filter_func),
        ops.do_action(update_storage)
    )

def aggregate_language_stats(storage):
    def update_stats(event):
        for lang in event.langs:
            storage[event.keyword][lang] += 1

    def to_lang_stats(keyword):
        return [
            {'keyword': keyword, 'lang': lang, 'count': count}
            for lang, count in storage[keyword].items()
        ]

    return lambda src: src.pipe(
        ops.do_action(update_stats),
        ops.map(lambda event: to_lang_stats(event.keyword)),
        ops.merge_all()
    )

def main():
    keyword = "python"
    new_repos_observable, lang_stats_observable = track_keyword(keyword)

    if new_repos_observable:
        new_repos_observable.subscribe(
            on_next=lambda event: print(f"New repo: {event}"),
            on_error=lambda error: print(f"Error: {error}"),
            on_completed=lambda: print("New repos tracking completed")
        )

    if lang_stats_observable:
        lang_stats_observable.subscribe(
            on_next=lambda event: print(f"Lang stats: {event}"),
            on_error=lambda error: print(f"Error: {error}"),
            on_completed=lambda: print("Language stats tracking completed")
        )

if __name__ == "__main__":
    main()
