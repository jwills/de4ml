docker run \
    -p 9090:9090 \
    -v $PWD/promconfig:/etc/prometheus \
    prom/prometheus
