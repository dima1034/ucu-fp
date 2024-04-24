from datetime import datetime, timedelta

import reactivex.operators as ops
from expression import curry
from reactivex import Observable

from message import GithubEvent

Keyword = str
RepoName = str


@curry(1)
def get_top_5_mentioned_projects(keyword: Keyword, src: Observable[GithubEvent]) -> Observable[RepoName]:
    return src.pipe(
        ops.filter(lambda event: event.keyword == keyword),
        ops.filter(lambda event: event.found_date >= datetime.now() - timedelta(days=1)),
        # ops.do_action(print),
        ops.to_list(),
        ops.map(lambda event_list: sorted(event_list, key=lambda event: event.stars_cnt, reverse=True)),
        ops.flat_map(lambda sorted_events: sorted_events[:5]),
        ops.map(lambda event: event.repo_fullname)
    )