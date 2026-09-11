import argparse
import json
import logging
import os

import boto3

from capstonellm.libs.client import StackOverflowClient
from capstonellm.models.models import (
    StackOverflowAnswer,
    StackOverflowQuestion,
)

logger = logging.getLogger(__name__)

AWS_S3_PATH="s3://dataminded-academy-capstone-llm-data/"
S3_USER = "Maria"


def save_json_to_s3(
    s3_client,
    data: dict,
    bucket: str,
    key: str,
) -> None:
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(
            data,
            ensure_ascii=False,
            indent=2,
        ),
        ContentType="application/json",
    )


def ingest(tag: str) -> None:
    client = StackOverflowClient()
    s3 = boto3.client("s3")

    s3_path = AWS_S3_PATH.rstrip("/")
    bucket = s3_path.removeprefix("s3://")

    logger.info(f"Using bucket: {bucket}")
    logger.info(f"Using tag: {tag}")

    # Get and validate questions
    question_items = client.get_questions(tag)

    questions = [
        StackOverflowQuestion(**item)
        for item in question_items
    ]

    # Get and validate answers
    answer_items = []

    for question in questions:
        answer_items.extend(
            client.get_answers(question.question_id)
        )

    answers = [
        StackOverflowAnswer(**item)
        for item in answer_items
    ]

    # S3 input location
    base_key = f"input/{S3_USER}/{tag}" 

    save_json_to_s3(
        s3,
        {"items": question_items},
        bucket,
        f"{base_key}/questions.json",
    )

    save_json_to_s3(
        s3,
        {"items": answer_items},
        bucket,
        f"{base_key}/answers.json",
    )

    logger.info(
        f"Saved {len(questions)} questions "
        f"and {len(answers)} answers"
    )

    logger.info(
        f"s3://{bucket}/{base_key}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="stackoverflow ingest"
    )

    parser.add_argument(
        "-t",
        "--tag",
        dest="tag",
        help="Tag of the question in Stack Overflow to process",
        default="python-polars",
        required=False,
    )

    args = parser.parse_args()

    logger.info("Starting the ingest job")

    ingest(args.tag)


if __name__ == "__main__":
    main()
