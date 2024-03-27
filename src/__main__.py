from datetime import datetime

import reactivex.operators as ops
from reactivex import from_iterable, Observable

import src.new_repo_processor as repo
from src.message import GithubEvent

if __name__ == "__main__":
    events = [
        GithubEvent(keyword="keyword1", repo_fullname="repo1", found_date=datetime.now(), match_cnt=1,
                    langs=["Java", "C++"]),
        GithubEvent(keyword="keyword2", repo_fullname="repo2", found_date=datetime.now(), match_cnt=1,
                    langs=["Java", "Go"]),
        GithubEvent(keyword="keyword2", repo_fullname="repo2", found_date=datetime.now(), match_cnt=1,
                    langs=["Java", "Go", "C++"]),
        GithubEvent(keyword="keyword1", repo_fullname="repo3", found_date=datetime.now(), match_cnt=1,
                    langs=["Java", "Javascript"]),
        GithubEvent(keyword="keyword1", repo_fullname="repo4", found_date=datetime.now(), match_cnt=1,
                    langs=["C++", "Javascript"])
    ]
    repos_storage = dict()
    new_repos = repo.filter_new_repos(repos_storage)
    # lang_stats_storage = dict()
    # lang_stats = lang.get_lang_stats(lang_stats_storage)
    source: Observable[GithubEvent] = from_iterable(events)

    new_repos(source).subscribe(print)
    # lang_stats(source).subscribe(print)

