FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir streamlit pandas plotly kafka-python

COPY app.py worker.py db.py ui.py ./

CMD python worker.py & streamlit run app.py --server.address 0.0.0.0 --server.headless true
