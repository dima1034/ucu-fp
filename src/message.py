import datetime
from dataclasses import dataclass


@dataclass
class GithubEvent:
    repo_fullname: str
    keyword: str
    found_date: datetime
    match_cnt: int
