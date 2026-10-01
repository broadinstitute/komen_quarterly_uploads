docker build --platform linux/amd64 -f Dockerfile.r -t komen_questionnaire_r:latest .
docker tag komen_questionnaire_r:latest us-central1-docker.pkg.dev/operations-portal-427515/komen/komen_questionnaire_r:latest
docker push us-central1-docker.pkg.dev/operations-portal-427515/komen/komen_questionnaire_r:latest
