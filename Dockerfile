FROM python:3.11-slim-buster

WORKDIR /root
ENV VENV /opt/venv
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8
ENV PYTHONPATH /root

RUN apt-get update && apt-get install -y build-essential 
# install pyenv
# RUN curl https://pyenv.run | bash
# RUN export PATH="${HOME}/.pyenv/bin:$PATH"
# RUN eval "$(pyenv init -)"
# RUN eval "$(pyenv virtualenv-init -)"
# RUN pyenv install 3.12.1
# RUN pyenv global 3.12.1

ENV VENV /opt/venv
# Virtual environment
RUN python -m venv ${VENV}
RUN echo $(python3.9 --version)
ENV PATH="${VENV}/bin:$PATH"

# Install Python dependencies
COPY requirements.txt /root
RUN pip install -r /root/requirements.txt

# Copy the actual code
COPY . /root

# This tag is supplied by the build script and will be used to determine the version
# when registering tasks, workflows, and launch plans
ARG tag
ENV FLYTE_INTERNAL_IMAGE $tag
