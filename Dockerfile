FROM python:3.11.9

USER root
# This makes it so pip runs as root, not the user.
ENV PIP_USER=false
ENV PIP_ROOT_USER_ACTION=ignore

# install build dependencies and needed tools
RUN apt-get update
RUN apt-get install -yq --no-install-recommends \
    wget \
    curl \
    gcc \
    g++ \
    python3 \
    python3-pip \
    sudo \
    make \
    dpkg \
    apt-transport-https \
    which \
    ca-certificates \
    gnupg

# Copy the entire project directory into the container
COPY . /app
RUN sudo pip3 install --upgrade pip\
  && sudo pip3 install --upgrade -r /app/requirements.txt


RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
RUN sudo apt-get update && sudo apt-get install google-cloud-cli -yq

ENV PATH=/usr/local/bin:$PATH
ENV PATH=$PATH:/usr/local/gcloud/google-cloud-sdk/bin
ENV PIP_USER=true

#install gcloud cli
ENV GPG_TTY=$(tty)

# Set the working directory to /app
WORKDIR /app