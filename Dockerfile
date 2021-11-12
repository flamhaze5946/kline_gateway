FROM flamhaze5946/lite-trade:20211112.1.0.0

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

WORKDIR /opt/kline_gateway
COPY config.py config.py
COPY fetcher.py fetcher.py
COPY pusher.py pusher.py
COPY subscriber.py subscriber.py
COPY utils.py utils.py
COPY main.py main.py
COPY config.json config.json

VOLUME ["config.json"]
ENTRYPOINT ["python", "main.py"]