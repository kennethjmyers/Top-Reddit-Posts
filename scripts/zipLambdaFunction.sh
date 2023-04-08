# This is meant to zip a lambda function with the reddit config
# use: sh zipLambdaFunction.sh -f someFunction
# saves zip to zippedLambdaFunction/someFunction.zip

# based on https://www.linkedin.com/pulse/add-external-python-libraries-aws-lambda-using-layers-gabe-olokun/
while getopts f: flag
do
    case "${flag}" in
        f) function_name=${OPTARG};;  # ie someFunction located in ../lambdaFunction/someFunction
    esac
done
: ${function_name:?Missing -f}   # checks if these have been set https://unix.stackexchange.com/questions/621004/bash-getopts-mandatory-arguments
echo "lambda function: $function_name";

[ -d "../lambdaFunctions/${function_name}" ] && echo "Directory ../lambdaFunctions/${function_name} exists." || { echo "Error: Directory ../lambdaFunctions/${function_name} does not exist."; exit 1; }

cd ./zippedLambdaFunction/
rm -r ./${function_name}
cp -r ../../lambdaFunctions/${function_name} ./
cp ../../reddit.cfg ./${function_name}/
rm -rf ${function_name}.zip # remove first if it exists
cd ./${function_name}/  # for some reason you have to zip from within this folder or it wont work, it otherwise wraps it in another folder
#rm -rf ./*.ipynb*  # remove any notebook stuff
zip -r ../${function_name}.zip * -x "*.ipynb*" "*pycache*"    # zip of function and the reddit config
cd ..
rm -r ./${function_name}  # clean up unzipped file
