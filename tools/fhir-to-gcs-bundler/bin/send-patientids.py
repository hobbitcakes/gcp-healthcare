import os
from concurrent import futures
from google.cloud import pubsub_v1

project_id = os.environ["src_data"]
topic_id = os.environ["src_topic"]
patient_ids_file = os.environ["patient_ids_file"]

# Configure the batch to publish as soon as there are 10 messages
# or 1 KiB of data, or 1 second has passed.
batch_settings = pubsub_v1.types.BatchSettings(
    max_messages=10,  # default 100
    max_bytes=1024,  # default 1 MB
    max_latency=1,  # default 10 ms
)
publisher = pubsub_v1.PublisherClient(batch_settings)
topic_path = publisher.topic_path(project_id, topic_id)
publish_futures = []

def load_patient_ids(filepath) -> list:
  with open(filepath) as f:
    return f.readlines()


# Resolve the publish future in a separate thread.
def callback(future: pubsub_v1.publisher.futures.Future) -> None:
    message_id = future.result()
    print(message_id)

def main():
  patientids = load_patient_ids(patient_ids_file)
  for patientid in patientids:
    message = patientid.strip().encode("utf-8")
    publish_future = publisher.publish(topic_path, message)
    # Non-blocking. Allow the publisher client to batch multiple messages.
    publish_future.add_done_callback(callback)
    publish_futures.append(publish_future)
  futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)

if __name__ == '__main__':
  main()
