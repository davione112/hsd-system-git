from flask import Flask
from threading import Event
import signal
from flask import stream_with_context, render_template, request, redirect, url_for, Response
import json


from flask_kafka import FlaskKafka
app = Flask(__name__)

INTERRUPT_EVENT = Event()

bus = FlaskKafka(INTERRUPT_EVENT,bootstrap_servers=",".join(["localhost:9092"]),group_id='hsd')

def listen_kill_server():
    signal.signal(signal.SIGTERM, bus.interrupted_process)
    signal.signal(signal.SIGINT, bus.interrupted_process)
    signal.signal(signal.SIGQUIT, bus.interrupted_process)
    signal.signal(signal.SIGHUP, bus.interrupted_process)

@bus.handle('hsd')
def hsd_handler(msg):
    record = json.loads(msg.value.decode('utf-8'))
    print(record)
    yield (f"data:{json.dumps(record)}\n\n")
    
@app.route('/')
def home():
    return render_template('secondPage.html')

@app.route('/chart-data', methods=['GET'])
def chart_data():
    return Response(hsd_handler(),mimetype="text/event-stream")

if __name__=='__main__':
    bus.run()
    listen_kill_server()
    app.run(debug=True)