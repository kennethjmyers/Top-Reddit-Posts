# Intended to be run from the model/ directory
# docker build -t predict-etl:latest .

# Based on info from https://github.com/mamba-org/micromamba-docker
# supplemental: https://blog.ceshine.net/post/replicate-conda-environment-in-docker/
FROM --platform=linux/amd64 mambaorg/micromamba:1.4.2

# Install packages
COPY . ./app
WORKDIR ./app
RUN micromamba install -y -n base -f ./environment.yml && \
    micromamba clean --all --yes

CMD python PredictETL.py
