# Intended to be run from the model/ directory
# docker build -t predict-etl:latest -f ./Dockerfile .
# docker run [-idt] --name predictETL --rm predict-etl
# test locally with:
#    docker run -it --name predictETL \
#      -e AWS_ACCESS_KEY_ID=[access key] \
#      -e AWS_SECRET_ACCESS_KEY=[secret key] \
#      --rm predict-etl

FROM predict-etl-packages:latest

COPY . .

# allows us to call executables directly on the path
ENV PATH=$PATH:/tmp/

# execute python script
CMD PredictETL.py
