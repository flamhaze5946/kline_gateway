FROM flamhaze5946/centos7-trade:20211024.1.0.0

COPY requirements.txt requirements.txt

RUN python3 -m pip install --upgrade pip
RUN pip3 install -r requirements.txt

WORKDIR /opt/kline_gateway
COPY config.py config.py
COPY fetcher.py fetcher.py
COPY pusher.py pusher.py
COPY subscriber.py subscriber.py
COPY utils.py utils.py
COPY main.py main.py
COPY config.json config.json

VOLUME ["config.json"]
ENTRYPOINT ["python3", "main.py"]