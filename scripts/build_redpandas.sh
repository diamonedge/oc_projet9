docker compose -f compose.yml down -v
docker compose -f compose.yml rm -v
docker compose -f compose.yml up -d --build redpanda
docker compose -f compose.yml run --rm redpanda-init
docker exec -it redpanda rpk topic create client_tickets -X brokers=localhost:9092 -p 3 -r 1 --if-not-exists
#docker exec -it redpanda rpk topic list -X brokers=localhost:9092
