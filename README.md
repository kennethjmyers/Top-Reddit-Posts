# Top Reddit Posts

[![codecov](https://codecov.io/gh/kennethjmyers/Top-Reddit-Posts/branch/main/graph/badge.svg?token=ACZEU30AHM)](https://codecov.io/gh/kennethjmyers/Top-Reddit-Posts)

![Python](https://img.shields.io/badge/python-3.7-blue.svg) 

![](./images/architecture-diagram.png)

This project intends to demonstrate a knowledge of:

1. Data Engineering and ETL - the collection and cleaning of data
2. Working within the AWS ecosystem - tools used include DynamoDB, Lambda functions, S3, RDS, EventBridge, IAM (managing permission sets, users, roles, policies, etc), ECR, and Fargate.
3. Data Science and Analysis - building out a simple model using collected data

**Make sure to read the [Wiki](https://github.com/kennethjmyers/Top-Reddit-Posts/wiki) for set-up instructions.**

## What is this?

This project collects data from rising posts on Reddit and identifies features that predict an upcoming viral post. It then automates the prediction process via a docker container deployed on AWS Fargate and notifies the users of potentially viral posts. 

Currently the model is a GBM model and it steps up the top 3% of posts based on testing data.

### Why? 

It takes a lot of time to scroll Reddit for new and rising posts to contribute to the conversation early enough where people might see it. The "Rising" view of r/pics typically has over 250 different posts reach it per day and many more than that are posted to the "New" view. However, only a handful of posts make it to the front page. 

Furthermore, by the time a post reaches the front page it can have hundreds of comments and very few people will scroll past the top few posts. Therefore it is important to get to a post early to increase the likelihood that your comment will rise to the top.

This project allows one to remove much of the time and effort in continuously searching Reddit for the next big posts, and allows one to limit comments to only posts with high probability of virality. The goal is to increase the chance that your voice will be heard. 

### Example

Below is a sample of [the first post](https://www.reddit.com/r/pics/comments/132ueaa/the_first_photo_of_the_chernobyl_plant_taken_by/) the bot found when it was deployed. When it first sent the notification, the post had only 29 comments but it went on to garner over 57k upvotes and over 1.4k comments. 

It is easy to see how advantageous it can be to find future viral posts before they hit the front page.

![](./images/bot-example.png)

### Results

When I started using the bot, my account of 12 years had 8,800 comment karma. For the first 10 days the model only predicted on r/pics posts and in those 10 days I had increased my comment karma to over 21,300, a 142% increase. To reiterate, **I more than doubled my comment karma of 12 years in just 10 days**.

On the 10th day I expanded the model to 8 more of the top subreddits (though the model had not been trained on these) and I used this regularly for days 10-20. During this time **I was averaging about 5,000 karma per day**. Used continuously this would be about 150k karma per month and 1.8MM karma/year. On multiple occasions my replies were given Reddit awards (which was also not something I'd received in the past) and I was given "achievement" flairs for being in the top 1% of karma earners.

![](./images/Karma_growth_vs_day.png)
 
 User results can vary, however, as it requires the user to be available when new notifications arrive and is dependent upon the user's replies and understanding of Reddit and the subreddits' rules and userbases.


#### Performance Monitoring

Performance monitoring is not yet a live process but can be found in the [monitoring notebook](model/Monitoring.ipynb).

![](./images/monitoring-precisions-recalls20230512.png)

The above plot shows precisions and recalls for various subreddits that the model has been operating on. The current version of the model was only trained on r/pics, as such it tends to perform the best there with about 45% recall and >80% precision. This is inline with the step up threshold, top 1%, that was selected. However, it performs worse for the other subreddits due to different subscriber sizes and activity levels. Data collection has been updated to plan for the next version of the model which will account for these variables. 

Furthermore, the next version of the model will attempt to be more flexible to the time of prediction. The current model mainly relies on the 40-60 minute time block and most predictions only come in during the last 10 minutes of a post's first hour. But often these posts already have dozens or, in rare cases, hundreds of comments so the model must become more agile for this.


## More Information

### Requirements

1. python == 3.7 (I have not tested this on other versions and can not guarantee it will work)
2. [AWS CLI v2](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)

### Components

1. Check out the [Getting Started](https://github.com/kennethjmyers/Top-Reddit-Posts/wiki/Getting-Started) section of the wiki for setting up your AWS account and local environment.
2. [Lambda function](./lambdaFunctions/getRedditDataFunction/) to collect data and store in DynamoDB. See [the Wiki](https://github.com/kennethjmyers/Top-Reddit-Posts/wiki/Lambda-Function---getRedditDataFunction) for setup instructions.
3. [ETL](model/ModelETL.py), [Analysis](./model/univariateAnalysis.ipynb) and [Model creation](model/model-GBM.ipynb). 
    1. Currently EMR is not being utilized for the ETL process but the ETL process was written in pyspark so that it could scale on EMR with growing data.  
    2. DynamoDB is not really meant for bulk read and writes. As such, it is not ideal for large ETL processes. It was chosen to demonstrate knowledge in an additional datastore and because it is available to the AWS free tier. When reading data from DynamoDB to Spark, I implemented data chunking to gather multiple DynamoDB partitions before they are distributed with Spark to improve reads efficiency.
    3. Model data and Model stored on S3.
4. [Docker image](model/Dockerfile) hosted on ECR and deployed on ECS via Fargate that automates [prediction ETL process](model/PredictETL.py), stage predicted results to Postgres database on RDS and send notifications via Discord to the user.

### A Note on Costs

This project tries to maximize the variety of tools to use in AWS while keeping costs low, particularly on the AWS Free Tier. 

If you are on the AWS free tier, then the primary cost is the use of Fargate. Currently, while returning results for a single subreddit every 10 minutes, the cost is about $0.20/day or about $6/month. 

Keep an eye on the costs though as this project uses S3, RDS, Lambda, and other services which are free within limits but will start to incur costs if you go beyond their trial limits or continue past the trial period.

In [April 2023 Reddit announced](https://www.nytimes.com/2023/04/18/technology/reddit-ai-openai-google.html) that they would be charging for API access in the future. This did not affect this project at time of creation but could affect others in the future.
