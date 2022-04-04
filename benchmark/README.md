# Benchmarking ASMUX

run nginx locally

```
nginx -p `pwd` -c benchmark/nginx.conf -g 'daemon off;'
```

run postgres

```
docker run -e POSTGRES_HOST_AUTH_METHOD=trust -p 5432:5432 postgres
```


run asmux in docker

```
docker run -it -v `pwd`:/opt/mautrix -p 29326:29326 asmux python3 -m mautrix_asmux -c /opt/mautrix/benchmark/config.yaml
```

run asmux locally

```
python -m mautrix_asmux -c benchmark/config.yaml
```
