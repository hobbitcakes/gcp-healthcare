#!/bin/bash
REGISTRY="gcf-artifacts"
IMAGE="${src_region}-docker.pkg.dev/${src_intake}/${REGISTRY}/fhir-to-gcs-bundler"
docker build --tag fhir-to-gcs-bundler:latest .
if [ "$?" -eq "0" ]; then
  docker tag fhir-to-gcs-bundler:latest $IMAGE
  docker push $IMAGE
fi
