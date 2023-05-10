#!/bin/bash
# Creates a lambda function with an existing role
# I made this so I could launch new and development functions with ease.
# For my purposes, the role was rather static for the jobs I might want to run,
# but this could be changed to make it a parameter.
# example
#   ./createLambdaFunction.sh -n getRedditDataFunction_dev -a 1234564789 -u AdministratorAccess -f ./zippedLambdaFunction/getRedditDataFunction.zip
# you may need to login first
#  aws sso login --profile AdministratorAccess

while getopts n:a:u:f: flag
do
    case "${flag}" in
        n) function_name=${OPTARG};;  # ie getRedditDataFunction_dev
        a) account_number=${OPTARG};;  # ie 1234564789
        u) aws_profile=${OPTARG};; # ie AdministratorAccess, think sso username
        f) zipfile=${OPTARG};;  #  ie ./zippedLambdaFunction/getRedditDataFunction.zip
    esac
done
: ${function_name:?Missing -n} ${account_number:?Missing -a} ${aws_profile:?Missing -u} ${zipfile:?Missing -f} # checks if these have been set https://unix.stackexchange.com/questions/621004/bash-getopts-mandatory-arguments
echo "function_name: $function_name";
echo "account_number: $account_number";
echo "sso profile: $aws_profile"
echo "zipfile: $zipfile"

# handler has form file.function
# the role is something I'd already made for lambda functions
# the layer is also something that was previously created in part with zipPythonPackage.sh
# I recommend the timout value should be 11*number of subreddits scraped (11s per subreddit), this is extreme, with 9 subs it needed ~13s
aws lambda create-function \
--function-name ${function_name} \
--zip-file fileb://${zipfile} \
--handler lambda_function.lambda_handler \
--runtime python3.7 \
--role arn:aws:iam::${account_number}:role/service-role/getRedditDataFunction-role-wg2skv5h \
--layers arn:aws:lambda:us-east-2:${account_number}:layer:praw:1 \
--timeout 100 \
--profile ${aws_profile} \
--region us-east-2

echo "Completed creating lambda function"