# Dagster libraries to run both dagster-webserver and the dagster-daemon. Does not
# need to have access to any pipeline code.

ARG PYTHON_VERSION=3.13
ARG DEBIAN_VERSION=trixie

FROM python:${PYTHON_VERSION}-slim-${DEBIAN_VERSION} AS base


# Create a non-root user for dagster

ENV HOME=/opt/dagster

ARG DOCKER_GROUP_ID

RUN groupadd --gid $DOCKER_GROUP_ID docker

RUN useradd --home-dir $HOME \
            --create-home \
            --system \
            --shell /usr/bin/bash \
            --gid nogroup \
            --groups docker \
            dagster



FROM base AS dagster

USER dagster

# Install Dagster

ARG DAGSTER_VERSION=1.12.0

WORKDIR $HOME
ENV PATH=$HOME/.local/bin:/usr/local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

RUN pip install \
        dagster==${DAGSTER_VERSION} \
        dagster-graphql==${DAGSTER_VERSION} \
        dagster-webserver==${DAGSTER_VERSION} \
        dagster-postgres \
        dagster-docker

# Set $DAGSTER_HOME and copy dagster instance and workspace YAML there
ENV DAGSTER_HOME=$HOME/dagster_home/

RUN mkdir -p $DAGSTER_HOME

COPY dagster.yaml workspace.yaml $DAGSTER_HOME

WORKDIR $DAGSTER_HOME
