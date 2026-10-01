# Komen Quarterly Processing 

## Docker
Anytime underlying Python code is changed, the Docker image needs to be rebuilt and pushed for the WDL to use. To 
build, tag, and push the Docker image, run the following commands to make sure permissions are correct:

```bash
gcloud auth login
gcloud config set project operations-portal-427515
```
To build, tag, and push the image you can run the below or you can run `./build_and_push_docker.sh` from the root of the repository which will do the same thing. Make sure to update the script if you want to change the image name or location.
```bash
docker build --platform linux/amd64 -t komen_quarterly_uploads:latest .
docker tag komen_quarterly_uploads:latest us-central1-docker.pkg.dev/operations-portal-427515/komen/komen_quarterly_uploads:latest
docker push us-central1-docker.pkg.dev/operations-portal-427515/komen/komen_quarterly_uploads:latest
```

_NOTE_: The Broad GCP project used above is a temporary location, and we'll need a permanent lcoation provided by 
the Komen team to push the image to. Once we have that, we'll need to update the WDL to point to the new location.

### R image (`Dockerfile.r`)

The `RunQuestionnaireAnalysis` task (part of `IngestKomenSamples`) runs `sfc_questionnaire_analysis_pipeline.R` in a separate image built from `Dockerfile.r`, since it needs an R runtime rather than Python. Anytime `sfc_questionnaire_analysis_pipeline.R`, `questionnaire_manifest.csv`, or `Dockerfile.r` itself changes, this image needs to be rebuilt and pushed too — run `./build_and_push_r_docker.sh` from the root of the repository, or the equivalent commands directly:
```bash
docker build --platform linux/amd64 -f Dockerfile.r -t komen_questionnaire_r:latest .
docker tag komen_questionnaire_r:latest us-central1-docker.pkg.dev/operations-portal-427515/komen/komen_questionnaire_r:latest
docker push us-central1-docker.pkg.dev/operations-portal-427515/komen/komen_questionnaire_r:latest
```
