#!/usr/bin/env python3
"""Simulate traffic from multiple apps querying MongoDB Atlas sample datasets.

Each app uses a distinct app name and injects an app-specific comment into every query.
"""

from __future__ import annotations

import argparse
import configparser
import json
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from pymongo import MongoClient
from pymongo.server_api import ServerApi


def load_config(config_path: Path) -> dict[str, Any]:
    with config_path.open("r", encoding="utf-8") as config_file:
        data = json.load(config_file)

    if "mongodb" not in data:
        raise ValueError("Config must include a mongodb section")

    if "apps" not in data or not isinstance(data["apps"], list):
        raise ValueError("Config must include an apps array")

    return data


def load_mongodb_uri_from_ini(ini_path: Path) -> str:
    parser = configparser.ConfigParser()
    parser.read(ini_path, encoding="utf-8")

    if not parser.has_section("mongodb"):
        raise ValueError(f"INI file '{ini_path}' must include [mongodb] section")

    uri = parser.get("mongodb", "uri", fallback="").strip()
    if not uri:
        raise ValueError(f"INI file '{ini_path}' must include mongodb.uri")

    return uri


def build_comment(app_name: str, app_comment: str, query_type: str) -> str:
    return f"app={app_name}; note={app_comment}; op={query_type}; source=traffic-simulator"


def run_query(client: MongoClient, app: dict[str, Any], query: dict[str, Any]) -> int:
    db_name = query["database"]
    coll_name = query["collection"]
    query_type = query.get("type", "find")
    app_name = app["name"]
    app_comment = app.get("comment", f"Traffic from {app_name}")

    db = client[db_name]
    collection = db[coll_name]
    comment = build_comment(app_name, app_comment, query_type)

    if query_type == "find":
        cursor = collection.find(
            filter=query.get("filter", {}),
            projection=query.get("projection"),
            limit=query.get("limit", 10),
            comment=comment,
        )
        return len(list(cursor))

    if query_type == "aggregate":
        cursor = collection.aggregate(
            pipeline=query.get("pipeline", []),
            comment=comment,
        )
        return len(list(cursor))

    if query_type == "count_documents":
        return collection.count_documents(
            filter=query.get("filter", {}),
            comment=comment,
        )

    if query_type == "distinct":
        values = collection.distinct(
            key=query["key"],
            filter=query.get("filter", {}),
            comment=comment,
        )
        return len(values)

    raise ValueError(f"Unsupported query type: {query_type}")


def enforce_min_query_duration(start_time: float, min_query_duration_ms: int) -> None:
    if min_query_duration_ms <= 0:
        return

    elapsed_ms = int((time.time() - start_time) * 1000)
    remaining_ms = min_query_duration_ms - elapsed_ms
    if remaining_ms > 0:
        time.sleep(remaining_ms / 1000)


def app_worker(
    app: dict[str, Any],
    mongodb_cfg: dict[str, Any],
    stop_at: float,
    sleep_range_ms: tuple[int, int],
    min_query_duration_ms: int,
) -> dict[str, Any]:
    app_name = app["name"]
    queries = app.get("queries", [])

    if not queries:
        return {"app": app_name, "requests": 0, "errors": 0, "last_error": "No queries configured"}

    # appname is visible in Atlas profiling and metrics for client attribution.
    client = MongoClient(
        mongodb_cfg["uri"],
        appname=app_name,
        server_api=ServerApi(mongodb_cfg.get("server_api_version", "1")),
    )

    requests = 0
    errors = 0
    last_error = ""

    try:
        while time.time() < stop_at:
            query = random.choice(queries)
            query_min_duration_ms = int(query.get("min_duration_ms", min_query_duration_ms))
            try:
                query_started_at = time.time()
                returned = run_query(client, app, query)
                enforce_min_query_duration(query_started_at, query_min_duration_ms)
                requests += 1
                print(f"[{app_name}] ok type={query.get('type', 'find')} returned={returned}")
            except Exception as exc:  # pylint: disable=broad-except
                errors += 1
                last_error = str(exc)
                print(f"[{app_name}] error: {exc}")

            sleep_ms = random.randint(sleep_range_ms[0], sleep_range_ms[1])
            time.sleep(sleep_ms / 1000)
    finally:
        client.close()

    return {"app": app_name, "requests": requests, "errors": errors, "last_error": last_error}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="MongoDB Atlas multi-app traffic simulator")
    parser.add_argument(
        "--config",
        default="simulator_config.json",
        help="Path to simulator config JSON file",
    )
    parser.add_argument(
        "--mongodb-ini",
        default="mongodb.ini",
        help="Path to INI file containing [mongodb] uri",
    )
    parser.add_argument(
        "--duration-seconds",
        type=int,
        default=None,
        help="Override simulation duration from config",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(Path(args.config))
    mongodb_uri = load_mongodb_uri_from_ini(Path(args.mongodb_ini))

    mongodb_cfg = dict(config["mongodb"])
    mongodb_cfg["uri"] = mongodb_uri
    sim_cfg = config.get("simulation", {})

    duration_seconds = args.duration_seconds or sim_cfg.get("duration_seconds", 60)
    sleep_range = sim_cfg.get("sleep_range_ms", [200, 1000])
    max_workers = sim_cfg.get("max_workers", 10)
    min_query_duration_ms = int(sim_cfg.get("min_query_duration_ms", 0))

    enabled_apps = [app for app in config["apps"] if app.get("enabled", True)]

    if not enabled_apps:
        raise ValueError("No enabled apps found in config")

    stop_at = time.time() + duration_seconds
    sleep_range_ms = (int(sleep_range[0]), int(sleep_range[1]))

    print(f"Starting simulation for {duration_seconds}s with {len(enabled_apps)} app(s)")

    lock = threading.Lock()
    total_requests = 0
    total_errors = 0

    with ThreadPoolExecutor(max_workers=min(max_workers, len(enabled_apps))) as executor:
        futures = [
            executor.submit(
                app_worker,
                app,
                mongodb_cfg,
                stop_at,
                sleep_range_ms,
                min_query_duration_ms,
            )
            for app in enabled_apps
        ]

        for future in as_completed(futures):
            result = future.result()
            with lock:
                total_requests += result["requests"]
                total_errors += result["errors"]

            print(
                "App summary "
                f"app={result['app']} requests={result['requests']} "
                f"errors={result['errors']} last_error={result['last_error']}"
            )

    print(f"Simulation complete total_requests={total_requests} total_errors={total_errors}")


if __name__ == "__main__":
    main()
