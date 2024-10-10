#!/bin/bash
REGISTRY="gcf-artifacts"
IMAGE="${src_region}-docker.pkg.dev/${src_intake}/${REGISTRY}/fhir-to-gcs-bundler"
gcloud --project=$src_intake beta run deploy fhir-to-gcs-bundler \
--image=${IMAGE}:latest \
--region=$src_region \
--no-allow-unauthenticated \
--timeout=300 \
--cpu=2 \
--memory=4Gi \
--no-cpu-throttling \
--max-instances=100 \
--concurrency=8 \
--set-env-vars "FHIR_STORE=${FHIR_STORE},BUCKET=${BUCKET}"
