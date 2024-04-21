from dataclasses import dataclass
from typing import Set, Dict
import threading

import reactivex.operators as ops
from expression import curry
from reactivex import Observable

from src.message import GithubEvent

Keyword = str
RepoName = str
RepoNameCache = Set[RepoName]
ReposPerKeywordStorage = Dict[Keyword, RepoNameCache]


@dataclass
class KeywordRepoNamePair:
    keyword: Keyword
    repo_name: RepoName


def _mapper(event: GithubEvent) -> KeywordRepoNamePair:
    return KeywordRepoNamePair(keyword=Keyword(event.keyword), repo_name=RepoName(event.repo_fullname))


def _filter_event(storage: ReposPerKeywordStorage, lock: threading.Lock, event: KeywordRepoNamePair) -> bool:
    with lock:
        return event.repo_name not in storage.get(event.keyword, set())


def _save_event(storage: ReposPerKeywordStorage, lock: threading.Lock, event: KeywordRepoNamePair) -> None:
    with lock:
        storage.setdefault(event.keyword, set()).add(event.repo_name)


@curry(1)
def filter_new_repos(storage: ReposPerKeywordStorage, src: Observable[GithubEvent]) -> Observable[KeywordRepoNamePair]:
    storage_lock = threading.Lock()
    return src.pipe(
        ops.map(_mapper),
        # ops.do_action(print),  # log
        ops.filter(lambda event: _filter_event(storage, storage_lock, event)),  # filter old results
        ops.do_action(lambda event: _save_event(storage, storage_lock, event)),  # save new results
        ops.do_action(on_error=print),
    )
