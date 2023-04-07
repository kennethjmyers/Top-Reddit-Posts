# This is a script to download a python package binaries, zip it, and upload it to an s3 bucket.
# The intention is to use that package as a layer for a lambda function (or something else).
# use: sh packagePython.sh -p praw -s [bucket name] -u [sso name]
# might need to login to your sso session first with: aws sso login --profile [sso name]

# based on https://www.linkedin.com/pulse/add-external-python-libraries-aws-lambda-using-layers-gabe-olokun/
while getopts p:s:u: flag
do
    case "${flag}" in
        p) package=${OPTARG};;  # ie praw=7.7.0
        s) s3_bucket=${OPTARG};;  # ie s3://your-s3-bucket-name/
        u) aws_profile=${OPTARG};; # ie AdministratorAccess, think sso username
    esac
done
: ${package:?Missing -p} ${s3_bucket:?Missing -s} ${aws_profile:?Missing -s}  # checks if these have been set https://unix.stackexchange.com/questions/621004/bash-getopts-mandatory-arguments
echo "package: $package";
echo "AWS Location: $awslocation";

bucketstatus=$(aws s3api head-bucket --bucket "${s3_bucket}" --profile "${aws_profile}" 2>&1)
if echo "${bucketstatus}" | grep 'Not Found';
then
  echo "bucket doesn't exist";
elif echo "${bucketstatus}" | grep 'Forbidden';
then
  echo "Bucket exists but not owned"
elif echo "${bucketstatus}" | grep 'Bad Request';
then
  echo "Bucket name specified is less than 3 or greater than 63 characters"
else
  echo "Bucket owned and exists";
fi
# probably need something that exits if it doesn't exist

mkdir -p ./zippedPackages/${package}/python

cd ./zippedPackages/${package}/python

# install binaries for package
pip install \
    --platform manylinux2014_x86_64 \
    --target=. \
    --implementation cp \
    --python 3.7 \
    --only-binary=:all: \
    --upgrade ${package}

rm -rf *dist-info  # some cleanup of unnecessary stuff
# zip package
cd ..
rm -rf ${package}.zip # remove first if it exists
zip -r ${package}.zip python  # zip contents of python to zip name
# move to aws, must have created before hand
aws s3 cp ${package}.zip s3://${s3_bucket}  --profile ${aws_profile}