import google.auth
import google.auth.transport.requests
import requests
import json
import os
import time
from google.cloud import storage
import functions_framework
from flask import Flask, request
import base64
import uuid

def generate_creds():
  creds, project = google.auth.default()
  auth_req = google.auth.transport.requests.Request()
  creds.refresh(auth_req)
  global bearer_token
  bearer_token = creds.token
  return bearer_token

app = Flask(__name__)
app.config["BEARER_TOKEN"] = generate_creds()

def get_env_vars():
  args = {}
  args['FHIR_STORE'] = os.environ['FHIR_STORE']
  args['BUCKET'] = os.environ['BUCKET']
  return args


def upload_blob_from_memory(bucket_name, contents, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    start = time.time()
    blob.upload_from_string(contents, timeout=360)
    end = time.time()
    print(f"{destination_blob_name} uploaded to {bucket_name} in {end - start}s")
    return "OK"

def fetch_resource_next_link(next_page_resources) :
    next_url = None
    for resources in next_page_resources :
         if "relation" in resources and resources['relation'] == 'next':
                return resources['url']

    return next_url

def extract_resources(entry_patient_resources) :
    data = [json.dumps(resource_patient["resource"]) for resource_patient in entry_patient_resources]
    print(f"Extracted {len(data)}  records")
    return data

def _get_helper(url, retries=0):
  headers = {"Content-Type": "application/fhir+json","Authorization": f"Bearer {app.config['BEARER_TOKEN']}"}
  if retries > 0:
    print(f"Retry number {retries} with simple backoff")
    time.sleep(retries * 3)
  start = time.time()
  response = requests.get(url, headers=headers)
  end = time.time() 
  print(f"GET request took {end - start}s to complete") 
  if response.status_code == 401:
    print("Possibly bad token, retrying once after refreshing the token.")
    app.config["BEARER_TOKEN"] = generate_creds()
    headers = {"Content-Type": "application/fhir+json","Authorization": f"Bearer {app.config['BEARER_TOKEN']}"}
    response = requests.get(url, headers=headers)
  if response.status_code == 429 and retries < 4:
    response = _get_helper(url, retries + 1)

  response.raise_for_status()
  return response

def _has_next_page(lpr):
  if "link" in lpr:
    for link in lpr["link"]:
      if link["relation"] == "next":
        return True

  return False 

def _get_all_pages(url):
  response = _get_helper(url)
  lpr = response.json()
  print(f"This patient has {lpr['total']} resources")
  resources = extract_resources(lpr["entry"])
  if  _has_next_page(lpr):
    print(f'Getting next page of results: {lpr["link"]}')
    resources.extend(_get_all_pages(fetch_resource_next_link(lpr["link"])))
  return resources

def get_patient_everything(patient_id, fhir_store):
  fhir_url = f'https://healthcare.googleapis.com/v1/{fhir_store}'
  request = f"{fhir_url}/fhir/Patient/{patient_id}/$everything?_count=1000"
  resources = _get_all_pages(request)
  print(f'Length of the resources array {len(resources)}')
  return resources 


def extract_id(message):
  '''
  Data schema: {"patient_id":"id"} where 'id' is the Resource.id of the patient
  '''
  base64_name = message["message"]["data"]
  data = base64.b64decode(base64_name).decode('utf-8')
  data_dict = json.loads(data)
  return data_dict["patient_id"]

def to_ndjson_format(resources):
  lines = ""
  for resource in resources:
    r = json.dumps(resource)
    line = resource +"\n"
    lines = lines + line

  return lines

@app.route("/", methods = ['POST'])
def main():
  start = time.time()
  args = get_env_vars()
  fhir_store = args["FHIR_STORE"]
  bucket = args["BUCKET"]
# This is bad, don't do this. It executes every invocation.
#  app.config["BEARER_TOKEN"] = generate_creds() 
  request_json = request.get_json()
  patient_id = extract_id(request_json)
  print(f"Starting {patient_id}")

  try:
    patient_lpr = get_patient_everything(patient_id, fhir_store)
  except Exception as e:
    return f"Failed because of {e.message}"

  ndjson = to_ndjson_format(patient_lpr)

  # Uncomment below line and comment the other to switch between
  # (non)deteriministic file names
  object_name = f"fhir-to-gcs-bundler/{str(uuid.uuid4())}.ndjson" # Possibly duplicates
  #object_name = f"fhir-to-gcs-bundler/{patient_id}.ndjson" # No duplicates
  upload_blob_from_memory(bucket, bytes(ndjson, encoding='utf8'), object_name)
  end = time.time()
  print(f"Finished {patient_id} in {end - start}s")


  return patient_id

if __name__ == "__main__":
  app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

