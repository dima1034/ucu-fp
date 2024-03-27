from datetime import datetime

import pytest
from reactivex import from_iterable, Observable

import src.new_repo_processor as processor
from src.message import GithubEvent


@pytest.fixture
def storage() -> dict:
    return {}


def test_filter_new_repos_stores_only_unique_repo_names_for_each_keyword(storage: dict) -> None:
    # GIVEN
    events = [
        GithubEvent(keyword="keyword1", repo_fullname="repo1", found_date=datetime.now(), match_cnt=1, langs=["Java"]),
        GithubEvent(keyword="keyword2", repo_fullname="repo2", found_date=datetime.now(), match_cnt=1, langs=["Java"]),
        GithubEvent(keyword="keyword2", repo_fullname="repo2", found_date=datetime.now(), match_cnt=1, langs=["Java"]),
        GithubEvent(keyword="keyword1", repo_fullname="repo3", found_date=datetime.now(), match_cnt=1, langs=["Java"])
    ]
    expected_storage = {
        "keyword1": {"repo1", "repo3"},
        "keyword2": {"repo2"}
    }
    source: Observable[GithubEvent] = from_iterable(events)

    # WHEN
    processor.filter_new_repos(storage)(source).subscribe()

    # THEN
    assert storage == expected_storage


def test_filter_new_repos_does_not_return_repos_found_in_storage(storage: dict) -> None:
    # GIVEN
    events = [
        GithubEvent(keyword="keyword1", repo_fullname="repo1", found_date=datetime.now(), match_cnt=1, langs=["Java"]),
        GithubEvent(keyword="keyword2", repo_fullname="repo2", found_date=datetime.now(), match_cnt=1, langs=["Java"]),
        GithubEvent(keyword="keyword2", repo_fullname="repo4", found_date=datetime.now(), match_cnt=1, langs=["Java"]),
        # New repo
        GithubEvent(keyword="keyword1", repo_fullname="repo3", found_date=datetime.now(), match_cnt=1, langs=["Java"])
    ]
    storage = {
        "keyword1": {"repo1", "repo3"},
        "keyword2": {"repo2"}
    }
    source: Observable[GithubEvent] = from_iterable(events)

    # WHEN
    result = []
    processor.filter_new_repos(storage)(source).subscribe(on_next=lambda e: result.append(e))

    # THEN
    assert len(result) == 1
    assert result[0].keyword == 'keyword2'
    assert result[0].repo_name == 'repo4'
