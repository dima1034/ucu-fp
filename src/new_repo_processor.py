from dataclasses import dataclass
from typing import Set, Dict

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


@curry(1)
def store_repo(storage: ReposPerKeywordStorage, src: Observable[GithubEvent]) -> Observable[None]:
    return src.pipe(
        ops.map(_mapper),
        ops.map(lambda event: storage.setdefault(event.keyword, set()).add(event.repo_name)),
        ops.do_action(on_error=print),
        ops.ignore_elements(),
    )


@curry(1)
def filter_new_repos(storage: ReposPerKeywordStorage, src: Observable[GithubEvent]) -> Observable[KeywordRepoNamePair]:
    return src.pipe(
        ops.map(_mapper),
        ops.filter(lambda event: event.repo_name not in storage.get(event.keyword, set())),
        ops.do_action(on_error=print),
    )


def subscribe_for_new_repo_updates(keyword: Keyword) -> Observable[RepoName]:
    pass
