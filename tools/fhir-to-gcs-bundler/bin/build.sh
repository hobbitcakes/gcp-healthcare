#!/bin/bash
REGISTRY="gcf-artifacts"
docker build --tag fhir-to-gcs-bundler:latest .
if [ "$?" -eq "0" ]; then
  docker tag fhir-to-gcs-bundler:latest ${src_region}-docker.pkg.dev/${src_intake}/${REGISTRY}/fhir-to-gcs-bundler
fi
