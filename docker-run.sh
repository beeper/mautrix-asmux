#!/bin/sh
cd /opt/mautrix-asmux
chown -R $UID:$GID /data /opt/mautrix-asmux
if [ -z "$DD_AGENT_HOST" ]; then
  exec su-exec $UID:$GID python3 -m mautrix_asmux -c /data/config.yaml
else
  exec su-exec $UID:$GID ddtrace-run python3 -m mautrix_asmux -c /data/config.yaml
fi
