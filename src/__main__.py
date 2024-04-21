import threading

from dotenv import load_dotenv
from reactivex import Observable, operators as ops
import reactivex
from reactivex.scheduler import ThreadPoolScheduler

import src.new_repo_processor as repo
import src.language_stats_processor as lang
from src.github_client import fetch_data_as_observable
from src.message import GithubEvent

load_dotenv()

thread_pool_scheduler = ThreadPoolScheduler(max_workers=10)
# new_thread_scheduler = NewThreadScheduler()

if __name__ == "__main__":
    words = ["python", "java"]

    repos_storage = dict()
    new_repos = repo.filter_new_repos(repos_storage)
    lang_stat = lang.get_lang_stats(dict())

    fetched_data: Observable[GithubEvent] = fetch_data_as_observable(words[0])
    fetched_data.pipe(ops.retry(5), ops.share(),
                      lambda src_of_message: reactivex.merge(new_repos(src_of_message),
                                                             lang_stat(src_of_message)),
                      ).subscribe(on_next=print, on_error=print)
