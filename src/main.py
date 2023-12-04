import os
import re
import json
import logging
import time

from dataclasses import dataclass

import schedule
import coloredlogs
import requests
from pymongo.database import Database
from pymongo.errors import OperationFailure
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pydantic import HttpUrl


from models import Repository


@dataclass
class Context:
    """Context object to pass around db connections"""

    db_repo_list: Database
    db_repos: Database
    github_token: str
    logger: logging.Logger


def get_repo_list(ctx: Context) -> list[HttpUrl]:
    """
    Returns a list of github repo urls from the repo-list db.
    """
    # get repo list from repo-list db
    repo_list = ctx.db_repo_list["repos"].find()
    # get url from each repo and convert to list
    repo_list = list(map(lambda x: HttpUrl(x["url"]), repo_list))
    return repo_list


def get_repo_name(url: HttpUrl) -> tuple[str, str]:
    """
    Returns the repo name and author from a github repo url.
    TODO: add error handling
    """
    # get repo name and author from url

    # remove http(s)://
    url_str: str = re.sub(r"https?://", "", url.path)
    # remove trailing slash
    url_str = re.sub(r"/$", "", url_str)
    url_strs = url_str.split("/")

    return url_strs[-2], url_strs[-1]


def construct_repo_from_api(api_response: dict) -> Repository:
    """
    Constructs a Repository object from a github api response.
    TODO: add error handling
    """
    # construct repo info from repo dict
    return Repository(
        name=api_response["name"],
        author=api_response["owner"]["login"],
        url=api_response["html_url"],
        description=api_response["description"],
        stars=api_response["stargazers_count"],
        language=api_response["language"],
        last_updated=api_response["updated_at"],
    )


def update_repo_stats(ctx: Context):
    # get repo list from repo-list db
    repo_list = get_repo_list(ctx)

    for rp in repo_list:
        # query github for repo info
        user, repo = get_repo_name(rp)
        r = requests.get(
            f"https://api.github.com/repos/{user}/{repo}",
            headers={"Authorization": f"token {ctx.github_token}"},
            timeout=5,
        )

        if r.status_code != 200:
            log_structured_message(ctx.logger, f"Error: {r.status_code} - {r.text}")
            continue

        info = r.json()
        # construct repo info
        repo_info = construct_repo_from_api(info)
        log_structured_message(ctx.logger, str(repo_info))

        # insert repo
        res = ctx.db_repos["repos"].insert_one(repo_info.model_dump())
        if not res.acknowledged:
            log_structured_message(ctx.logger, "Error: failed to insert repo")
            continue


def log_structured_message(log, message, **kwargs):
    """
    Helper function to create a structured log message
    """
    log_message = {"message": message, **kwargs}
    log.info(json.dumps(log_message))


# main
if __name__ == "__main__":
    # setup logging
    logger = logging.getLogger(__name__)
    coloredlogs.install(
        level=logging.INFO,
        logger=logger,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    db_username = os.environ.get("DB_USERNAME")
    db_password = os.environ.get("DB_PASSWORD")

    uri = (
        f"mongodb+srv://{db_username}:{db_password}@nvim-plugin-list.kuxk7uc.mongodb.net/"
        "?retryWrites=true&w=majority"
    )

    # setup db connection
    client = MongoClient(uri, server_api=ServerApi("1"), uuidRepresentation="standard")
    try:
        client.admin.command("ping", check=True)
        log_structured_message(logger, "Successfully connected to the Atlas Cluster")
    except OperationFailure as e:
        log_structured_message(
            logger, "Unable to connect to the Atlas Cluster, error:", kwargs=str(e)
        )

    context = Context(
        db_repos=client["repos"],
        db_repo_list=client["repo-list"],
        github_token=os.environ.get("GITHUB_TOKEN"),
        logger=logger,
    )

    # start update loop
    while True:
        schedule.every(5).minutes.do(update_repo_stats, context)
        schedule.run_pending()
        time.sleep(1)
