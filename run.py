import argparse
import datetime as dt
from typing import Iterable
import subprocess


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dag", default=None)
    parser.add_argument("--since", default=None)
    parser.add_argument("--until", default=None)
    args = parser.parse_args()
    args.since = dt.datetime.strptime(args.since, "%Y-%m-%d").date()
    args.until = dt.datetime.strptime(args.until, "%Y-%m-%d").date()
    return args


def get_dates(since, until) -> Iterable[dt.date]:
    if until < since:
        raise ValueError("Until date must be greater or equal to since.")
    for day in range(0, int((until - since).days + 1), 1):
        yield since + dt.timedelta(days=day), since + dt.timedelta(days=day + 1)


if __name__ == "__main__":
    args = get_args()
    for since, until in get_dates(args.since, args.until):
        cmd = [
            "airflow",
            "dags",
            "backfill",
            args.dag,
            "-s",
            since.strftime("%Y-%m-%d"),
            "-e",
            until.strftime("%Y-%m-%d"),
        ]
        subprocess.run(cmd)
