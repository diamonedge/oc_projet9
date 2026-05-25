echo "################################################# Cleaning data part"
sudo rm -rf data/
mkdir -p data/checkpoint/ticket_stream_processor/
mkdir -p data/output/enriched_tickets/
mkdir -p data/output/latest/tickets_by_priority/
mkdir -p data/output/latest/tickets_by_type/

echo "################################################ Building dockers from local"
date
docker compose down
docker compose rm
docker rm producer_container
docker ps -a
#docker compose build --no-cache spark-processor producer redpanda
docker compose build spark-processor producer repanda

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
