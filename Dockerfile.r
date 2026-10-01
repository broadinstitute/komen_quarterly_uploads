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

# gtsummary/gt aren't part of rocker/tidyverse's preinstalled package set. Installing them
# from plain CRAN source alone can leave the base image's pre-baked rendering chain
# (xfun/knitr/rmarkdown/commonmark) mismatched with what gt/gtsummary expect — surfaces at
# render time as e.g. "object 'attr' is not exported by 'namespace:xfun'". Installing the
# whole chain together as pre-built binaries from a single Posit Package Manager snapshot
# (scoped to this image's Ubuntu jammy base) guarantees a mutually consistent version set
# without recompiling every outdated package in the image from source.
RUN Rscript -e "install.packages(c('xfun', 'knitr', 'rmarkdown', 'commonmark', 'gt', 'gtsummary'), repos = 'https://packagemanager.posit.co/cran/__linux__/jammy/latest')" \
    && Rscript -e "for (p in c('xfun', 'knitr', 'rmarkdown', 'commonmark', 'gt', 'gtsummary')) cat(p, ':', as.character(packageVersion(p)), '\n')"

WORKDIR /app
COPY sfc_questionnaire_analysis_pipeline.R /app/sfc_questionnaire_analysis_pipeline.R
COPY questionnaire_manifest.csv /app/questionnaire_manifest.csv
