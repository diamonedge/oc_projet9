uv run --env-file .env python producer/ticket_producer.py --limit 5
docker exec -it redpanda rpk topic consume client_tickets --brokers localhost:9092 --num 5
