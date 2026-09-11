import argparse
import json
import logging
import os

import boto3

#from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    coalesce,
    collect_list,
    explode,
    lit,
    struct,
)

#from capstonellm.common.catalog import llm_bucket
from capstonellm.common.spark import ClosableSparkSession

AWS_S3_PATH="s3://dataminded-academy-capstone-llm-data/"
S3_USER = "Maria"

logger = logging.getLogger(__name__)


def clean(session, env, tag):
    # Base path 
    s3_path = AWS_S3_PATH.removeprefix("s3://").rstrip("/")
    bucket = s3_path
    
    logger.info("bucket:", bucket)

    # Paths used by Spark
    input_path = f"s3a://{bucket}/input/{S3_USER}/{tag}"
    output_path = f"cleaned/{S3_USER}/{tag}"

    questions_path = f"{input_path}/questions.json"
    answers_path = f"{input_path}/answers.json"

    logger.info(f"Questions path: {questions_path}")
    logger.info(f"Answers path: {answers_path}")

    # read questions
    questions = (
        session.read
        .option("multiLine", True)
        .json(questions_path)
        .select(explode("items").alias("question"))
        .select("question.*")
        .select("question_id", "title", "body")
        .withColumnRenamed("body", "question_body")
    )

    # read and clean answers
    # only those with non negative score
    # keep is_accepted flag to check 'best possible' answer
    answers = (
        session.read
        .option("multiLine", True)
        .json(answers_path)
        .select(explode("items").alias("answer"))
        .select("answer.*")
        .filter("score >= 0")
        .select(
            "answer_id",
            "question_id",
            "body",
            "score",
            "is_accepted",
        )
        .withColumnRenamed("body", "answer_body")
        .withColumn(
            "is_accepted",
            coalesce("is_accepted", lit(False)),
        )
    )

    # join questions + answers
    #only questions with answers
    joined = questions.join(
        answers,
        on="question_id",
        how="inner",
    )

    # group answers by question
    cleaned = (
        joined
        .groupBy(
            "question_id",
            "title",
            "question_body",
        )
        .agg(
            collect_list(
                struct(
                    "answer_id",
                    "answer_body",
                    "score",
                    "is_accepted",
                )
            ).alias("answers")
        )
    )

    # Write one JSON per question
    s3 = boto3.client("s3")

    logger.info(f"Input questions: {questions.count()}")
    logger.info(f"Input answers: {answers.count()}")

    rows = cleaned.collect()

    for row in rows:
        question_id = row["question_id"]

        data = {
            "question_id": question_id,
            "title": row["title"],
            "question_body": row["question_body"],
            "answers": [
                {
                    "answer_id": answer["answer_id"],
                    "answer_body": answer["answer_body"],
                    "score": answer["score"],
                    "is_accepted": answer["is_accepted"],
                }
                for answer in row["answers"]
            ],
        }

        key = f"{output_path}/question_{question_id}.json"

        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(
                data,
                ensure_ascii=False,
                indent=2,
            ),
            ContentType="application/json",
        )

    logger.info(
        "Cleaned data written to %s",
        output_path,
    )


def main():
    parser = argparse.ArgumentParser(
        description="capstone_llm"
    )

    parser.add_argument(
        "-e",
        "--env",
        dest="env",
        help="environment we are executing in",
        required=False,
        default="local",
    )

    parser.add_argument(
        "-t",
        "--tag",
        dest="tag",
        help="the tag to process",
        default="python-polars",
        required=False,
    )

    logger.info("starting the cleaning job")

    args = parser.parse_args()

    common_spark_config = {
        "spark.hadoop.fs.s3a.impl":
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider":
            "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider",
    }

    if args.env == "local":
        print(
            "This is a local execution of the capestonellm project"
        )

        builder = (
            SparkSession.builder
            .appName("Spark S3 Integration")
            .config(
                "spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.4.2",
            )
        )

        for key, value in common_spark_config.items():
            builder = builder.config(key, value)

        session = builder.getOrCreate()

        clean(
            session,
            args.env,
            args.tag,
        )

    else:
        with ClosableSparkSession(
            "capstone_llm",
            spark_config=common_spark_config,
        ) as session:
            clean(
                session,
                args.env,
                args.tag,
            )


if __name__ == "__main__":
    main()
