import os
from os.path import join, dirname

from dotenv import load_dotenv

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

DELAY = 0.01
NUM_PARTITIONS = 3
KAFKA_BROKER = "localhost:9092"
TRANSACTIONS_TOPIC = "transactions"
TRANSACTIONS_CONSUMER_GROUP = "transactions"
ANOMALIES_TOPIC = "classifier"
ANOMALIES_CONSUMER_GROUP = "classifier"

SLACK_API_TOKEN = os.environ.get("SLACK_API_TOKEN")
SLACK_CHANNEL = "anomalies-alerts"
