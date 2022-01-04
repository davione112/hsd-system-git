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
consumer1 = KafkaConsumer('transaction', bootstrap_servers=['localhost:9092'])
app = Flask(__name__)

df = pd.DataFrame({'sentiment':['OFFENSIVE','CLEAN','HATE'],'count':[0,0,0]})     

@app.route('/')
def home():
    return render_template('secondPage.html')

@app.route('/chart-data', methods=['GET','POST'])
def chart_data():
    return Response(get_stream_data_count(),mimetype="text/event-stream")

@app.route('/table-data',methods=['GET'])
def table_data():
    return Response(get_stream_data(),mimetype="text/event-stream")

def get_stream_data():
    try:
        for msg in consumer:
            print('received')
            record = json.loads(msg.value.decode('utf-8'))
            print(record)
            yield (f"data:{json.dumps(record)}\n\n")
    finally:
        consumer.close()
        print('consumer closed')

def get_stream_data_count():
    try:
        for msg in consumer1:
            print('received')
            record = json.loads(msg.value.decode('utf-8'))
            print(record)
            if len(record.keys()) < 3:
                df.loc[df['sentiment'] == record['sentiment'],'count'] = record['count']
                print(df)
                continue
            yield (f"data:{json.dumps(record)}\n\n")
    finally:
        consumer.close()
        print('consumer closed')
            
if __name__ == "__main__":
    app.run(debug=True)
