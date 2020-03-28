FROM docker.io/alpine:3.11

RUN apk add --no-cache \
      py3-aiohttp \
      py3-ruamel.yaml \
      py3-attrs \
      py3-idna \
      # Other dependencies
      ca-certificates \
      su-exec

COPY requirements.txt /opt/mautrix-asmux/requirements.txt
WORKDIR /opt/mautrix-asmux
RUN apk add build-base python3-dev && pip3 install -r requirements.txt && apk del build-base python3-dev

COPY . /opt/mautrix-asmux
RUN pip3 install .

ENV UID=1337 GID=1337
VOLUME /data

CMD ["/opt/mautrix-asmux/docker-run.sh"]
