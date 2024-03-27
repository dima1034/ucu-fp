import datetime
from dataclasses import dataclass
from typing import List


@dataclass
class GithubEvent:
    repo_fullname: str
    keyword: str
    found_date: datetime
    match_cnt: int
    langs: List[str]
