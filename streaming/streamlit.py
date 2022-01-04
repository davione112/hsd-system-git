import streamlit as st

consumer = KafkaConsumer('hsd', bootstrap_servers=['localhost:9092'])
