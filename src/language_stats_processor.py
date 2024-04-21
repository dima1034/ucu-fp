import threading
from dataclasses import dataclass
from typing import Dict, List, Iterator

import reactivex.operators as ops
from expression import curry
from reactivex import Observable, from_iterable

from src.message import GithubEvent

Keyword = str
Lang = str
LangCounter = Dict[Lang, int]
LangCountersPerKeyWordStorage = Dict[Keyword, LangCounter]


@dataclass
class LangStats:
    keyword: Keyword
    lang: Lang
    cnt: int


def _mapper(event: GithubEvent) -> List[LangStats]:
    return [LangStats(event.keyword, lang, 1) for lang in event.langs]


def _update_storage(storage: LangCountersPerKeyWordStorage, lock: threading.Lock, stats: LangStats) -> None:
    with lock:
        keyword_stats = storage.setdefault(stats.keyword, {})
        keyword_stats[stats.lang] = keyword_stats.get(stats.lang, 0) + stats.cnt


def _get_latest_snapshot(storage: LangCountersPerKeyWordStorage, lock: threading.Lock) -> List[LangStats]:
    result = []
    with lock:
        for keyword in storage.keys():
            stats_per_lang = storage[keyword]
            for lang in stats_per_lang.keys():
                result.append(LangStats(keyword, lang, stats_per_lang[lang]))

    return result


@curry(1)
def get_lang_stats(storage: LangCountersPerKeyWordStorage, src: Observable[GithubEvent]) -> Observable[LangStats]:
    storage_lock = threading.Lock()
    return src.pipe(
        ops.flat_map(lambda event: from_iterable(_mapper(event))),
        ops.group_by(lambda stats: (stats.keyword, stats.lang)),
        ops.flat_map(
            lambda grp: grp.pipe(ops.reduce(lambda acc, cur: LangStats(cur.keyword, cur.lang, acc.cnt + cur.cnt)))
        ),
        ops.do_action(lambda stats: _update_storage(storage, storage_lock, stats)),
        ops.last(),
        ops.flat_map(lambda _: from_iterable(_get_latest_snapshot(storage, storage_lock))),
    )
