docker build --platform linux/amd64 -t komen_quarterly_uploads:latest .
docker tag komen_quarterly_uploads:latest us-central1-docker.pkg.dev/operations-portal-427515/komen/komen_quarterly_uploads:latest
docker push us-central1-docker.pkg.dev/operations-portal-427515/komen/komen_quarterly_uploads:latest
