import json
import base64
from jinja2 import Environment, PackageLoader, select_autoescape

def random_ADTA03(adt_template) -> str:
  msg = adt_template.render()
  return msg

def ingest_hl7v2_message(
    project_id, location, dataset_id, hl7v2_store_id, hl7v2_message_file
):
    """Creates a new HL7v2 message from a template and ingestes it into the hl7V2Store via REST.

    See https://github.com/GoogleCloudPlatform/python-docs-samples/tree/main/healthcare/api-client/v1/hl7v2
    before running the sample."""
    # Imports the Google API Discovery Service.
    from googleapiclient import discovery

    # Imports Python's built-in "json" module
    import json

    api_version = "v1"
    service_name = "healthcare"
    # Returns an authorized API client by discovering the Healthcare API
    # and using GOOGLE_APPLICATION_CREDENTIALS environment variable.
    client = discovery.build(service_name, api_version)

    hl7v2_parent = f"projects/{project_id}/locations/{location}"
    hl7v2_store_name = "{}/datasets/{}/hl7V2Stores/{}".format(
        hl7v2_parent, dataset_id, hl7v2_store_id
    )

    with open(hl7v2_message_file) as hl7v2_message:
        hl7v2_message_content = json.load(hl7v2_message)

    request = (
        client.projects()
        .locations()
        .datasets()
        .hl7V2Stores()
        .messages()
        .ingest(parent=hl7v2_store_name, body=hl7v2_message_content)
    )

    response = request.execute()
    print(f"Ingested HL7v2 message from file: {hl7v2_message_file}")
    return response


if __name__ == "__main__":
 # project_id = 'my-project'  # replace with your GCP project ID
 # location = 'us-central1'  # replace with the parent dataset's location
 # dataset_id = 'my-dataset'  # replace with the HL7v2 store's parent dataset ID
 # hl7v2_store_id = 'my-hl7v2-store'  # replace with the HL7v2 store's ID
 # hl7v2_message_file = 'hl7v2-message.json'  # replace with the path to the HL7v2 file

  # Start Templating
  msg_templates = ["ADT_A03.hl7"]
  templates = []

  env = Environment(loader=PackageLoader("hl7_random_sender"), autoescape=select_autoescape())

  for msg_template in msg_templates:
    template = env.get_template(msg_template)

  msg = f'{message}'
  ingest_hl7v2_message(project, location, dataset, v2_store, msg)
