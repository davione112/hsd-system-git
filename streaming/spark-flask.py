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

consumer = KafkaConsumer('anomalies', bootstrap_servers=['localhost:9092'], auto_offset_reset='latest')
app = Flask(__name__)
url = 'https://www.youtube.com/watch?v=sH4hzS6dfOo'

@app.route('/home')
def home():
    return render_template('firstPage.html',data = url)

@app.route('/table-data', methods=['GET','POST'])
def table_data():
    return Response(get_stream_data(),mimetype="text/event-stream")

def get_stream_data():
    try:
        for msg in consumer:
            print('received')
            record = json.loads(msg.value.decode('utf-8'))
            print(record)
            # if len(record.keys()) < 3:
            #     df.loc[df['sentiment'] == record['sentiment'],'count'] = record['count']
            #     print(df)
            #     continue
            yield (f"data:{json.dumps(record)}\n\n")
    finally:
        consumer.close()
        print('consumer closed')
            
if __name__ == "__main__":
    app.run(debug=True)
