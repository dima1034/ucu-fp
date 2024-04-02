from flask import Flask, jsonify, request
import requests
import time
from collections import defaultdict
from dotenv import load_dotenv
import os
from reactivex import Observable
from reactivex.scheduler import ThreadPoolScheduler
from message import GithubEvent
import datetime

app = Flask(__name__)

# GitHub API configuration
GITHUB_API_URL = "https://api.github.com"
GITHUB_ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
REQUESTS_PER_MINUTE = 30 

# In-memory data store (replace with a proper database for production)
project_data = defaultdict(lambda: defaultdict(int))
language_stats = defaultdict(int)


def search_github(keyword):
    headers = {
        "Authorization": f"Bearer {GITHUB_ACCESS_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }

    url = f"{GITHUB_API_URL}/search/code?q={keyword}"
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code}")
        return None


def get_repo_language(languages_url):
    headers = {
        "Authorization": f"Bearer {GITHUB_ACCESS_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }

    response = requests.get(languages_url, headers=headers)

    if response.status_code == 200:
        languages = response.json()
        if languages:
            return max(languages, key=languages.get)

    return None


def process_search_results(keyword, results):
    for item in results["items"]:
        repo_full_name = item["repository"]["full_name"]
        language = get_repo_language(item["repository"]["languages_url"])
        project_data[keyword][repo_full_name] += 1
        language_stats[language] += 1
        yield GithubEvent(
            repo_fullname=repo_full_name,
            keyword=keyword,
            found_date=datetime.datetime.now(),
            match_cnt=project_data[keyword][repo_full_name],
            langs=[language] if language else []
        )


def get_top_projects(keyword, limit=5):
    projects = project_data[keyword]
    sorted_projects = sorted(
        projects.items(), key=lambda x: x[1], reverse=True)
    return sorted_projects[:limit]


def get_language_stats():
    return dict(language_stats)


@app.route("/track", methods=["POST"])
def track_keyword():
    keyword = request.json["keyword"]
    results = search_github(keyword)
    if results:
        events = process_search_results(keyword, results)
        observable = Observable.from_iterable(events)
        observable.subscribe(on_next=lambda event: print(event))
    return jsonify({"message": f"Tracking started for keyword: {keyword}"})


@app.route("/stats/<keyword>", methods=["GET"])
def get_keyword_stats(keyword):
    top_projects = get_top_projects(keyword)
    language_statistics = get_language_stats()

    return jsonify({
        "top_projects": top_projects,
        "language_stats": language_statistics
    })


if __name__ == "__main__":
    app.run()
