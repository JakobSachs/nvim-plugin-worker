import os
import sys
import re
import json
import logging

from datetime import datetime

from dataclasses import dataclass

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
    path = url.path
    if not path:
        raise Exception(f"Error: failed to get repo name from {url}")

    url_str: str = re.sub(r"https?://", "", path)
    # remove trailing slash
    url_str = re.sub(r"/$", "", url_str)
    url_strs = url_str.split("/")

    return url_strs[-2], url_strs[-1]


def get_README_from_api(user: str, repo: str, branch: str) -> str | None:
    # try main branch
    url = f"https://raw.githubusercontent.com/{user}/{repo}/{branch}/README.md"
    r = requests.get(url, timeout=5)
    if r.status_code == 200:
        return r.text
    return None


def construct_repo_from_api(ctxt: Context, api_response: dict) -> Repository | None:
    """
    Constructs a Repository object from a github api response.
    TODO: add error handling
    """
    # construct repo info from repo dict
    try:
        repo = Repository(
            name=api_response["name"],
            author=api_response["owner"]["login"],
            url=api_response["html_url"],
            description=api_response["description"],
            stars=api_response["stargazers_count"],
            language=api_response["language"].title(),
            last_updated=api_response["updated_at"],
        )
    except Exception as e:
        log_structured_message(
            ctxt.logger, f"Error: failed to construct repo from api response: {e}"
        )
        return None

    return repo


def create_repo_in_db(ctx: Context, rp: HttpUrl, exists: bool = False):
    """
    Creates a repo in the db from a github repo url.
    Assumes that the repo does not already exist in the db.
    """
    # query github for repo info
    user, repo = get_repo_name(rp)

    r = requests.get(
        f"https://api.github.com/repos/{user}/{repo}",
        headers={"Authorization": f"token {ctx.github_token}"},
        timeout=5,
    )

    if r.status_code != 200:
        raise Exception(f"Error: failed to get repo info for {rp} from GitHub API")

    info = r.json()

    readme = get_README_from_api(user, repo, info["default_branch"])
    if readme is None:
        raise Exception(f"Error: failed to get readme for {rp} from GitHub")

    # construct repo info
    repo_info = construct_repo_from_api(ctx, info)

    if repo_info is None:
        raise Exception(f"Error: failed to construct repo from api response")

    # add readme to repo
    repo_info.readme = readme

    # rename id to _id (since mongo uses _id and pydantic wont let me use underscore)
    dump = repo_info.model_dump()
    dump["_id"] = dump.pop("id")

    if exists:
        # remove _id from dump
        dump.pop("_id")
        # update repo
        res = ctx.db_repos["repo"].update_one(
            {"url": str(rp)}, {"$set": dump}, comment="updated from scheduled job"
        )
        if not res.acknowledged:
            raise Exception(f"Error: Failed to update repo in db: {res}")
    else:
        # insert repo
        res = ctx.db_repos["repo"].insert_one(
            dump, comment="inserted from scheduled job"
        )
        if not res.acknowledged:
            raise Exception(f"Error: Failed to insert repo into db: {res}")


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
        logger.info("Successfully connected to the Atlas Cluster")
    except OperationFailure as op_e:
        logger.error(f"Unable to connect to the Atlas Cluster, error: {str(op_e)}")
        sys.exit(1)

    context = Context(
        db_repos=client["repos"],
        db_repo_list=client["repo-list"],
        github_token=os.environ.get("GITHUB_TOKEN", ""),
        logger=logger,
    )

    log_structured_message(logger, "Starting scheduled job")
    repos: list[HttpUrl] = get_repo_list(context)

    # NOTE: Yes i could parrallelize this but i dont want to get rate limited,
    # plus im lazy and this runs like a few times a day so its not a big deal
    for repo in repos:  # process each repo
        # check if repo already exists in db
        update = False
        res = context.db_repos["repo"].find_one({"url": str(repo)})
        if res is not None:  # update repo
            log_structured_message(
                logger, f"Repo already exists in db: {repo}, {res['_id']}"
            )
            update = True

            # update star count in history
            star_update = {
                "repo_id": res["_id"],
                "timestamp": datetime.now(),
                "stars": res["stars"],
            }
            res = context.db_repos["stars_history"].insert_one(star_update)
            if not res.acknowledged:
                raise Exception(f"Error: Failed to insert star update into db: {res}")


        try:  # create repo in db
            create_repo_in_db(context, repo, exists=update)
        except Exception as e:
            log_structured_message(logger, f"Failed to process: {e}")
            continue
