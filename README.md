# Top Reddit Posts

## Purpose

This project intends to demonstrate a knowledge of:

1. Data Engineering and ETL - the collection and cleaning of data
2. Working within the AWS ecosystem - tools used include DynamoDB, Lambda functions, S3, RDS, EventBridge, IAM and IAM Identity Center (managing permission sets, users, roles, policies, etc)
3. Data Science and Analysis - building out a simple model using collected data

## What is this?

This project collects data from rising posts on Reddit and identify features that predict an upcoming viral post. Why? Consider someone who wanted to farm comment karma by getting to a viral post relatively early and voicing their input or humor. One could gather that data and notify themselves if a submission showed viral promise. I have no intention of actually using it in this way but it was the hypothetical around which I built this project. 

## Requirements

1. python == 3.7
2. [praw](https://github.com/praw-dev/praw) == 7.7.0
3. [AWS CLI v2](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
4. Spark == 3.0.1

## Components

1. Set up lambda function to collect data and store in DynamoDB ([getRedditDataFunction](./lambdaFunctions/getRedditDataFunction/)).
2. Model creation, ETL, and analysis of collected data ([model](./model/)). Model and Model Data stored on S3. ETL performed in Spark to scale with growing data.
3. Set up lambda function that automates ETL process, stage new data to Postgres database on RDS and send notifications via SNS.
