import logging
import base64
import os
import json
import functions_framework
from google.cloud import pubsub_v1

# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def main(cloud_event):
    # Print out the data from Pub/Sub, to prove that it worked
    resource = json.loads(base64.b64decode(cloud_event.data["message"]["data"]))
    if resource["resourceType"] == 'Observation':
      logging.debug(resource)
      new_attr = extract_attributes(resource)
      print(f'Potential attributes: {new_attr}')
      project_id = os.environ.get("PROJECT_ID")
      topic_id = os.environ.get("OUTPUT_TOPIC")
      print('About to requeue onto the Pub/Sub Topic.')
      publish(project_id, topic_id, json.dumps(resource).encode('utf-8'), new_attr)

def extract_attributes(resource: json) -> dict:
  new_attributes = {}
  categories = extract_category(resource)
  codes = extract_codes(resource)
  # Uniqueness not preserved
  for category in categories:
    new_attributes.update(category)
  # Uniqueness not preserved
  for code in codes:
    new_attributes.update(code)

  return new_attributes

def extract_category(resource: json) -> list:
  categories = []
  for category in resource['category']:
    logging.debug(f'Category: {category}')
    for code in category['coding']:
      logging.debug(f'Code: {code}')
      categories.append({code['system']: code['code']})

  return categories

def extract_codes(resource: json) -> list:
  codes = []
  for code in resource['code']['coding']:
    codes.append({code['system'] : code['code']})

  return codes

def publish(project_id, topic, message, attributes=None):
  publisher = pubsub_v1.PublisherClient()
  topic_path = publisher.topic_path(project_id, topic)

  if attributes:
    future = publisher.publish(topic_path, message, **attributes)
  else:
    future = publisher.publish(topic_path, message)
  
  return future.result()
  
