# Intended to be run from the model/ directory
# docker build -t predict-etl:latest -f ./Dockerfile .
# docker run [-idt] --name predictETL --rm predict-etl

FROM predict-etl-packages:latest

CMD /opt/conda/bin/python /tmp/PredictETL.py

# I originally wanted to use this but it would run into issues when it got to pyspark
#ENTRYPOINT [ "/opt/conda/bin/python" ]
#CMD [ "/tmp/PredictETL.py" ]
