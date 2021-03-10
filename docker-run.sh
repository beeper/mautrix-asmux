#!/bin/sh
cd /opt/mautrix-asmux
chown -R $UID:$GID /data /opt/mautrix-asmux
exec su-exec $UID:$GID python3 -m mautrix_asmux -c /data/config.yaml
