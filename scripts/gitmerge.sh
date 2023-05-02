#!/bin/bash
# This script will merge main into the current branch
# This is basically rebasing to the current origin/main
# https://stackoverflow.com/a/17141512/5034651
# to run this simply do
#   ./gitmerge.sh
# if not executable run
#   chmod +x ./gitmerge.sh

while getopts b: flag
do
    case "${flag}" in
        b) branch=${OPTARG};;  # ie: my-branch
    esac
done
: ${branch:?Missing -b}   # checks if these have been set https://unix.stackexchange.com/questions/621004/bash-getopts-mandatory-arguments
echo "branch: $branch";

git checkout main
git pull origin main
git checkout $branch
git branch -m ${branch}-old  # rename the branch
git checkout main
git checkout -b $branch
git merge --squash ${branch}-old  # merge old branch to the new one
git commit
git branch -D ${branch}-old  # delete old branch
