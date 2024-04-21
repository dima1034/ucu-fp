import threading
from dotenv import load_dotenv
from reactivex import Observable, operators as ops
from reactivex.scheduler import ThreadPoolScheduler

import src.new_repo_processor as repo
from src.github_client import fetch_data_as_observable
from src.message import GithubEvent

load_dotenv()

thread_pool_scheduler = ThreadPoolScheduler(max_workers=10)

if __name__ == "__main__":
    repos_storage = dict()
    new_repos = repo.filter_new_repos(repos_storage)
    source: Observable[GithubEvent] = fetch_data_as_observable("python")
    new_repos_obs = new_repos(source).pipe(ops.observe_on(thread_pool_scheduler))

    new_repos_obs.subscribe(
        on_next=lambda event: print(f"New repo (Thread: {threading.current_thread().name}): {event.repo_name}"),
        on_error=lambda error: print(f"Error in new repos observable: {error}"),
        on_completed=lambda: print("New repos tracking completed")
    )
