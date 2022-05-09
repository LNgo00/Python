FROM python:3.8-slim-buster
RUN pip install telethon mysql-connector-python redis

WORKDIR /usr/src/birdbrain
COPY ["scripts/*", "./"]

ENTRYPOINT ["/usr/local/bin/python3"]
