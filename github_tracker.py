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
from expression import curry
from rx import from_iterable
from rx.scheduler import ThreadPoolScheduler

load_dotenv()

# GitHub API configuration
GITHUB_API_URL = "https://api.github.com"
GITHUB_ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
REQUESTS_PER_MINUTE = 30  # Adjust based on GitHub API rate limits

# In-memory data store (replace with a proper database for production)
project_data = defaultdict(lambda: defaultdict(int))
language_stats = defaultdict(int)

Keyword = str
RepoName = str
RepoNameCache = Set[RepoName]
ReposPerKeywordStorage = Dict[Keyword, RepoNameCache]
Lang = str
LangCounter = Dict[Lang, int]
LangCountersPerKeyWordStorage = Dict[Keyword, LangCounter]

@dataclass
class GithubEvent:
    repo_fullname: str
    keyword: str
    found_date: datetime.datetime
    match_cnt: int
    langs: List[str]

@dataclass
class KeywordRepoNamePair:
    keyword: Keyword
    repo_name: RepoName

@dataclass
class KeywordLangPair:
    keyword: Keyword
    langs: List[Lang]

@dataclass
class LangStats:
    keyword: Keyword
    lang: Lang
    cnt: int

def _mapper(event: GithubEvent) -> KeywordRepoNamePair:
    return KeywordRepoNamePair(keyword=Keyword(event.keyword), repo_name=RepoName(event.repo_fullname))

def filter_new_repos(storage: ReposPerKeywordStorage):
    def _filter_new_repos(src: Observable[GithubEvent]) -> Observable[KeywordRepoNamePair]:
        return src.pipe(
            ops.map(_mapper),
            ops.filter(lambda event: event.repo_name not in storage.get(event.keyword, set())),
            ops.do_action(lambda event: storage.setdefault(event.keyword, set()).add(event.repo_name)),
            ops.do_action(on_error=print),
        )
    return _filter_new_repos

def _lang_mapper(event: GithubEvent) -> KeywordLangPair:
    return KeywordLangPair(keyword=Keyword(event.keyword), langs=[Lang(lang) for lang in event.langs])

def get_lang_stats(storage: LangCountersPerKeyWordStorage):
    def _get_lang_stats(src: Observable[GithubEvent]) -> Observable[LangStats]:
        def _update_lang_stats(event: KeywordLangPair):
            for lang in event.langs:
                storage.setdefault(event.keyword, defaultdict(int))[lang] += 1

        return src.pipe(
            ops.map(_lang_mapper),
            ops.do_action(_update_lang_stats),
            ops.flat_map(lambda event: [LangStats(keyword=event.keyword, lang=lang, cnt=cnt)
                                        for lang, cnt in storage[event.keyword].items()]),
        )
    return _get_lang_stats

def search_github(keyword):
    headers = {
        "Authorization": f"Bearer {GITHUB_ACCESS_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }
    url = f"{GITHUB_API_URL}/search/code?q={keyword}"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code}")
        return None

def get_repo_language(languages_url):
    headers = {
        "Authorization": f"Bearer {GITHUB_ACCESS_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }
    response = requests.get(languages_url, headers=headers)
    if response.status_code == 200:
        languages = response.json()
        if languages:
            return max(languages, key=languages.get)
    return None

def process_search_results(keyword, results):
    for item in results["items"]:
        repo_full_name = item["repository"]["full_name"]
        language = get_repo_language(item["repository"]["languages_url"])
        project_data[keyword][repo_full_name] += 1
        language_stats[language] += 1
        yield GithubEvent(
            repo_fullname=repo_full_name,
            keyword=keyword,
            found_date=datetime.datetime.now(),
            match_cnt=project_data[keyword][repo_full_name],
            langs=[language] if language else []
        )

def track_keyword(keyword):
    results = search_github(keyword)
    if results:
        events = process_search_results(keyword, results)
        observable = from_iterable(events)

        new_repos_storage = defaultdict(set)
        new_repos_observable = observable.pipe(filter_new_repos(new_repos_storage))

        lang_stats_storage = defaultdict(lambda: defaultdict(int))
        lang_stats_observable = observable.pipe(get_lang_stats(lang_stats_storage))

        return new_repos_observable, lang_stats_observable
    return None, None

def main():
    keyword = "python"
    new_repos_observable, lang_stats_observable = track_keyword(keyword)

    if new_repos_observable:
        new_repos_observable.subscribe(on_next=lambda event: print(f"New repo: {event}"))
    if lang_stats_observable:
        lang_stats_observable.subscribe(on_next=lambda event: print(f"Lang stats: {event}"))

if __name__ == "__main__":
    main()