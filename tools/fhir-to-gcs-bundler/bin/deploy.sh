#!/bin/bash
REGISTRY="gcf-artifacts"
IMAGE="${src_region}-docker.pkg.dev/${src_intake}/${REGISTRY}/fhir-to-gcs-bundler"
docker push $IMAGE 
gcloud run deploy fhir-to-gcs-bundler \
--image=$IMAGE:latest \
--region=us-central1 \
--project=${src_intake} \
--set-env-vars "FHIR_STORE=${FHIR_STORE},BUCKET=${BUCKET}"
