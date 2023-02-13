import os
import requests

webhook_url = os.environ['WEBHOOK_URL']

def send_webhook_notification(data):
  requests.post(webhook_url, json=data)
