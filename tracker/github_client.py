from typing import Dict, List, Iterator
from dataclasses import dataclass


from reactivex import from_iterable, Observable

@dataclass
class GithubEvent:
    repo_fullname: str
    keyword: str
    found_date: datetime.datetime
    match_cnt: int
    langs: list

GITHUB_API_URL = "https://api.github.com"
REQUESTS_PER_MINUTE = 30


def _get_authorization_headers() -> Dict:
    return {
        "Authorization": f"Bearer {os.getenv('ACCESS_TOKEN')}",
        "Accept": "application/vnd.github.v3+json"
    }


def _fetch_github_api(url: str) -> Dict:
    response = requests.get(url, headers=_get_authorization_headers())
    assert response.status_code == 200, f"status code {response.status_code}"
    return response.json()


def _get_languages(languages_url: str) -> List[str]:
    languages_data = _fetch_github_api(languages_url)
    return list(languages_data.keys()) if languages_data else []


def _fetch_data_as_iterator(keyword: str) -> Iterator[GithubEvent]:
    url = f"{GITHUB_API_URL}/search/code?q={keyword}"
    results = _fetch_github_api(url)
    for item in results.get("items", []):
        yield GithubEvent(
            repo_fullname=item["repository"]["full_name"],
            keyword=keyword,
            found_date=datetime.datetime.now(),
            stars_cnt=random.randint(1, 10000),  # for simplicity since API requires a lot of page requests
            langs=_get_languages(item["repository"]["languages_url"])
        )


def fetch_data_as_observable(keyword: str) -> Observable[GithubEvent]:
    return from_iterable(_fetch_data_as_iterator(keyword))