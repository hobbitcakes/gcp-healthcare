#!/bin/bash
REGISTRY="gcf-artifacts"
IMAGE="${src_region}-docker.pkg.dev/${src_intake}/${REGISTRY}/fhir-to-gcs-bundler"
gcloud --project=$src_intake beta run deploy fhir-to-gcs-bundler \
--image=${IMAGE}:latest \
--region=$src_region \
--no-allow-unauthenticated \
--timeout=3600 \
--cpu=8 \
--memory=8Gi \
--no-cpu-throttling \
--max-instances=500 \
--concurrency=8 \
--set-env-vars "FHIR_STORE=${FHIR_STORE},BUCKET=${BUCKET}"
