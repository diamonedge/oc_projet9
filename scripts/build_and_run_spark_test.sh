#!/bin/sh

echo "################################################# Cleaning data part"
sudo chown -R "$(id -u):$(id -g)" data || true
rm -rf data/output/* data/checkpoint/*
mkdir -p data/output data/checkpoint

echo "################################################ Building dockers from local"
date
docker compose down || true
docker compose rm -f || true
docker rm -f producer_container 2>/dev/null || true
docker ps -a
#docker compose build --no-cache spark-processor producer redpanda
docker compose build spark-processor producer redpanda

echo "################################################ Prepping tests"
date
docker compose up -d redpanda
docker compose up -d redpanda-init
docker compose up -d spark-processor

echo "################################################ Target dir is empty"
ls -lsah data/output/latest/tickets_by_type/ 2>/dev/null || true

echo "################################################ Test"
date
docker compose run --name producer_container producer
date

echo "################################################# Waiting for Spark to produce output"
until ls data/output/latest/tickets_by_type/*.json >/dev/null 2>&1
do
  sleep 0.2
done

echo "################################################## Checking output"
date
ls -lsah data/output/latest/tickets_by*/
cat data/output/latest/tickets_by_type/*.json
cat data/output/latest/tickets_by_priority/*.json 2>/dev/null || true
date

echo "########################################### End of test ##################################"
