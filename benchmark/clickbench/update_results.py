#!/usr/bin/env python3
# coding: utf-8


import json
import glob
import logging
import argparse

from jinja2 import Environment, FileSystemLoader

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
TEMPLATE_FILE = "index.jinja"


def update_results(dataset, title, url):
    queries = []
    for query_file in sorted(glob.glob(f"{dataset}/queries/*.sql")):
        with open(query_file, "r") as f:
            queries.append(f.read())
    results = []
    for result_file in glob.glob(f"results/{dataset}/**/*.json", recursive=True):
        logger.info(f"reading result: {result_file}...")
        with open(result_file, "r") as f:
            results.append(json.load(f))

    logger.info("loading report template %s ...", TEMPLATE_FILE)
    templateLoader = FileSystemLoader(searchpath="./")
    templateEnv = Environment(loader=templateLoader)
    template = templateEnv.get_template(TEMPLATE_FILE)
    logger.info("rendering result with args: %s ...", args)
    outputText = template.render(
        dataset=dataset,
        title=title,
        url=url,
        queries=queries,
        results=results,
    )
    with open(f"results/{dataset}.html", "w") as f:
        f.write(outputText)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="generate html results")
    parser.add_argument("--dataset", required=True, help="dataset name")
    parser.add_argument("--pr", help="benchmark pull request number")
    parser.add_argument("--release", help="benchmark release tag")
    args = parser.parse_args()

    title = f"ClickBench - {args.dataset} - "
    url = "https://github.com/datafuselabs/databend/"
    if args.pr:
        title += f"PR #{args.pr}"
        url += f"pull/{args.pr}"
    elif args.release:
        title += f"Release {args.release}"
        url += f"releases/tag/{args.release}"
    else:
        raise Exception("either --pr or --release is required")
    update_results(args.dataset, title, url)
