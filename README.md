# Komen Quarterly Processing 

## Docker
Anytime underlying Python code is changed, the Docker image needs to be rebuilt and pushed for the WDL to use. To 
build, tag, and push the Docker image, run the following commands:

```bash
docker build -t komen_quarterly_uploads:latest .
docker tag komen_quarterly_uploads:latest us-central1-docker.pkg.dev/operations-portal-427515/komen/komen_quarterly_uploads:latest
docker push us-central1-docker.pkg.dev/operations-portal-427515/komen/komen_quarterly_uploads:latest
```

_NOTE_: The Broad GCP project used above is a temporary location, and we'll need a permanent lcoation provided by 
the Komen team to push the image to. Once we have that, we'll need to update the WDL to point to the new location.
