#!/bin/bash

# Our env vars. Modify these for yourself
. ./env.sh

# Copy the config
PRIMARY_PATH="projects/{$IMPORT_PROJECT}/locations/${IMPORT_LOCATION}/datasets/${IMPORT_DATASET}/fhirStores/${IMPORT_FHIR_STORE}"
curl -s -X GET -H "Authorization: Bearer $(gcloud auth print-access-token)" "https://healthcare.googleapis.com/v1/${PRIMARY_PATH}"
# Modify the Config
#
# Create the FHIR Store

echo -e "${EXPORT_DATASET}"


