#!/bin/bash
# This script runs gitmerge.sh to update the notebooks branch and push the new branch to origin/notebooks
# The purpose of this script is to keep the notebooks branch one commit ahead of the main branch
# to run this simply do
#   ./updateNotebooksBranch.sh
# if not executable run
#   chmod +x ./updateNotebooksBranch.sh

./gitmerge.sh -b notebooks  # this will merge notebooks with main
git checkout notebooks
git push origin notebooks --force
