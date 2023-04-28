# Intended to be run from the model/ directory
# docker build -t predict-etl:latest -f ./Dockerfile .
# docker run [-idt] --name predictETL [image id]

FROM predict-etl-packages

ENTRYPOINT [ "/opt/conda/bin/python" ]
CMD [ "/tmp/PredictETL.py" ]