from dataclasses import dataclass
from typing import Dict, List

import reactivex.operators as ops
from expression import curry
from reactivex import Observable

from message import GithubEvent

Keyword = str
Lang = str
LangCounter = Dict[Lang, int]
LangCountersPerKeyWordStorage = Dict[Keyword, LangCounter]


@dataclass
class KeywordLangPair:
    keyword: Keyword
    langs: List[Lang]


@dataclass
class LangStats:
    keyword: Keyword
    lang: Lang
    cnt: int


def _mapper(event: GithubEvent) -> KeywordLangPair:
    return KeywordLangPair(keyword=Keyword(event.keyword), langs=[Lang(lang) for lang in event.langs])


@curry(1)
def get_lang_stats(storage: LangCountersPerKeyWordStorage, src: Observable[GithubEvent]) -> Observable[LangStats]:
    pass
