import threading

from dotenv import load_dotenv
from reactivex import operators as ops
import reactivex
import asyncio
from reactivex.scheduler import ThreadPoolScheduler

import new_repo_processor as repo
import language_stats_processor as lang
import top_mentioned_projects_processor as top
from github_client import fetch_data_as_observable_async

load_dotenv()

thread_pool_scheduler = ThreadPoolScheduler(max_workers=10)

if __name__ == "__main__":
    
    words = ["BaLiKfromUA", "dima1034", "java"]

    repos_storage = dict()
    new_repos = repo.filter_new_repos(repos_storage)
    lang_stat = lang.get_lang_stats(dict())

    async def main():

        fetched_data = await fetch_data_as_observable_async(words[0])
        thread_pool_scheduler = ThreadPoolScheduler(max_workers=10)

        repos_storage = dict()
        new_repos = repo.filter_new_repos(repos_storage)
        lang_stat = lang.get_lang_stats(dict())
        top_5 = top.get_top_5_mentioned_projects(words[0])

        fetched_data.pipe(
            ops.retry(5),
            ops.share(),
            lambda src: reactivex.merge(new_repos(src), lang_stat(src), top_5(src)),
            ops.observe_on(thread_pool_scheduler),
        ).subscribe(
            on_next=lambda event: print(
                f"New event (Thread: {threading.current_thread().name}): {event}"
            ),
            on_error=print,
            on_completed=lambda: print("Completed!"),
            scheduler=thread_pool_scheduler,
        )

        await asyncio.sleep(1)

    asyncio.run(main())
