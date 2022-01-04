from flask import stream_with_context, Flask, render_template, request, redirect, url_for, Response

import json
import os
import time
import pandas as pd
import numpy as np
from kafka import KafkaConsumer
import socket
import logging
import threading

consumer = KafkaConsumer('hsd', bootstrap_servers=['localhost:9092'])
app = Flask(__name__)

@app.route('/')
def home():
    return render_template('secondPage.html')

@app.route('/chart-data', methods=['GET'])
def chart_data():
    return Response(get_stream_data(),mimetype="text/event-stream")

def get_stream_data():
    try:
        for msg in consumer:
            print('received')
            record = json.loads(msg.value.decode('utf-8'))
            yield (f"data:{json.dumps(record)}\n\n")
    except ValueError:
        print('Error')
        consumer.close()
            
if __name__ == "__main__":
    app.run(debug=True)