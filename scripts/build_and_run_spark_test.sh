echo "################################################# Cleaning data part"
sudo chown -R "$(id -u):$(id -g)" data
rm -rf data/output/* data/checkpoint/*
mkdir -p data/output data/checkpoint

echo "################################################ Building dockers from local"
date
docker compose down
docker compose rm
docker rm producer_container
docker ps -a
docker compose build --no-cache spark-processor producer redpanda
#docker compose build spark-processor producer repanda

echo "################################################ Prepping tests"
date
docker compose up -d redpanda
docker compose up -d redpanda-init
docker compose up -d spark-processor

echo "################################################ Target dir is empty"
ls -lsah data/output/latest/tickets_by_type/

echo "################################################ Test"
date
docker compose run --name producer_container producer
date

echo "################################################# Waiting for Spark to produce output"
while [ ! -f data/output/latest/tickets_by_type/*.json ]
do
  sleep .2
done

echo "################################################## Checking output"
date
ls -lsah data/output/latest/tickets_by*/
cat data/output/latest/tickets_by_type/*.json
date

echo "########################################### End of test ##################################"
