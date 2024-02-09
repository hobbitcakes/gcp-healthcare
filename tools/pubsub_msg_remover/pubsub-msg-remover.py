#!/usr/bin/env python3
import os
from google.cloud import pubsub_v1

NAMES = []
ACKED = []

def load_names(file) -> list:
  """Load a file into a list. Expects each line is the Pub/Sub message's data element."""
  with open(file, 'r') as f:
    global NAMES
    NAMES = [line.rstrip() for line in f]

def callback(message):
  print(message.data.decode('UTF-8'))
  if message.data.decode('UTF-8') in NAMES:
    print(f'ACK\'ing {message.data}')
    message.ack()
    ACKED.append(message.data.decode('UTF-8'))

def ack_names(subscription):
  subscriber = pubsub_v1.SubscriberClient()
  future = subscriber.subscribe(subscription, callback=callback)

  halt_number = 1000
  i = 0
  with subscriber:
    try:
      future.result()
      i += 1
    except Exception as e:
      print(f'Error {e}')

    if i >= halt_number:
      return

if __name__ == '__main__':
  subscription = 'projects/YOUR_PROJECT/subscriptions/SUBSCRIPTION_NAME'
  file = './v2_messages_to_remove.list'

  load_names(file)
  print(f'NAMES: {NAMES}')
  ack_names(subscription)
  print(f'ACKED: {ACKED}')




