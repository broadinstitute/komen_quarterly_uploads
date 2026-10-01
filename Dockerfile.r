FROM rocker/tidyverse:4.4.1

USER root

# install build deps + gcloud CLI (gsutil is used to pull a release's CSVs at task runtime)
RUN apt-get update && apt-get install -yq --no-install-recommends \
    curl \
    apt-transport-https \
    ca-certificates \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
    && apt-get update && apt-get install -yq google-cloud-cli \
    && rm -rf /var/lib/apt/lists/*

# gtsummary/gt aren't part of rocker/tidyverse's preinstalled package set
RUN Rscript -e "install.packages(c('gtsummary', 'gt'), repos = 'https://cloud.r-project.org')"

WORKDIR /app
COPY sfc_questionnaire_analysis_pipeline.R /app/sfc_questionnaire_analysis_pipeline.R
