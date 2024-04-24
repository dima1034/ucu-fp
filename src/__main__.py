import threading

from dotenv import load_dotenv
from reactivex import Observable, operators as ops
import reactivex
from reactivex.scheduler import ThreadPoolScheduler

import new_repo_processor as repo
import language_stats_processor as lang
from github_client import fetch_data_as_observable_async
from message import GithubEvent
from reactivex.operators import retry, share
import asyncio

load_dotenv()

thread_pool_scheduler = ThreadPoolScheduler(max_workers=10)
# new_thread_scheduler = NewThreadScheduler()

if __name__ == "__main__":
    # BaLiKfromUA github_pat_11
    words = ["BaLiKfromUA", "java"]

    repos_storage = dict()
    new_repos = repo.filter_new_repos(repos_storage)
    lang_stat = lang.get_lang_stats(dict())

    async def main():

        fetched_data = await fetch_data_as_observable_async(words[0])

        thread_pool_scheduler = ThreadPoolScheduler(max_workers=10)

        fetched_data.pipe(
            ops.retry(5),
            ops.share(),
            lambda src: reactivex.merge(new_repos(src), lang_stat(src)),
            ops.observe_on(thread_pool_scheduler),
        ).subscribe(
            on_next=lambda event: print(
                f"New event (Thread: {threading.current_thread().name}): {event}"
            ),
            on_error=print,
            on_completed=lambda: print("Completed!"),
        )

        await asyncio.sleep(5)

    asyncio.run(main())
