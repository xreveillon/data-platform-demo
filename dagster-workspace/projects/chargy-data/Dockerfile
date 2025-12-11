# Dagster libraries to run both dagster-webserver and the dagster-daemon. Does not
# need to have access to any pipeline code.

ARG PYTHON_VERSION=3.13
ARG DEBIAN_VERSION=trixie

FROM python:${PYTHON_VERSION}-slim-${DEBIAN_VERSION}


# Create a non-root user for dagster

ENV HOME=/opt/dagster
ENV DAGSTER_HOME=$HOME/dagster_home

RUN useradd --home-dir $HOME \
            --create-home \
            --system \
            --shell /usr/bin/bash \
            --user-group \
            --uid 999 \
            dagster

USER dagster

# Set $DAGSTER_APP
ENV DAGSTER_APP=$HOME/chargy_app/
RUN mkdir -p $DAGSTER_APP

# Install uv and code dependencies

WORKDIR $HOME
ENV PATH=$HOME/.local/bin:/usr/local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

COPY --chown=dagster:dagster --from=ghcr.io/astral-sh/uv:latest /uv /uvx $HOME/.local/bin/

#TODO: Find chich libraries are development-only. Search for uv dependency groups, or for uv.toml, to differentiate between dev and prod packages.

COPY --chown=dagster:dagster uv.lock pyproject.toml $DAGSTER_APP

RUN cd $DAGSTER_APP && \
    uv sync

RUN mkdir -p $DAGSTER_HOME/{storage,.telemetry}
RUN touch $DAGSTER_HOME/dagster.yaml


# Copy code

COPY --chown=dagster:dagster src $DAGSTER_APP/src

WORKDIR $DAGSTER_APP

EXPOSE 4000

ENTRYPOINT [ "uv" , "run" , "--offline", "--no-dev" ]

CMD [ "dagster", "code-server", "start", "-h", "0.0.0.0", "-p", "4000", "--working-directory", "src", "--module-name", "chargy.definitions" ]
