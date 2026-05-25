import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer


REQUESTS = {
    "technical": [
        "Problème de connexion à l'espace client",
        "Erreur lors de l'utilisation de l'application",
        "Incident sur un équipement industriel",
        "Problème d'accès aux données",
    ],
    "billing": [
        "Question sur une facture",
        "Demande de duplicata de facture",
        "Erreur de montant facturé",
    ],
    "commercial": [
        "Demande d'information sur une offre",
        "Demande de contact commercial",
        "Question sur un contrat",
    ],
    "account": [
        "Modification des informations du compte",
        "Réinitialisation du mot de passe",
        "Création d'un nouvel accès utilisateur",
    ],
    "incident": [
        "Service indisponible",
        "Alerte critique sur une chaîne de traitement",
        "Perte temporaire d'accès à une application",
    ],
}

PRIORITIES = ["low", "medium", "high", "critical"]
PRIORITY_WEIGHTS = [0.45, 0.35, 0.15, 0.05]


def getenv_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value else default


def getenv_float(name: str, default: float) -> float:
    value = os.getenv(name)
    return float(value) if value else default


def build_ticket(client_count: int) -> dict:
    request_type = random.choice(list(REQUESTS.keys()))

    return {
        "ticket_id": f"TCK-{uuid.uuid4().hex[:12].upper()}",
        "client_id": f"CLI-{random.randint(1, client_count):05d}",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "request": random.choice(REQUESTS[request_type]),
        "request_type": request_type,
        "priority": random.choices(PRIORITIES, weights=PRIORITY_WEIGHTS, k=1)[0],
    }


def delivery_report(error, message) -> None:
    if error is not None:
        print(f"[ERROR] Message non livré : {error}")
        return

    print(
        "[OK] Message livré "
        f"topic={message.topic()} "
        f"partition={message.partition()} "
        f"offset={message.offset()}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Producteur de tickets clients JSON pour Redpanda."
    )

    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("REDPANDA_BOOTSTRAP_SERVERS", "localhost:19092"),
    )

    parser.add_argument(
        "--topic",
        default=os.getenv("REDPANDA_TOPIC", "client_tickets"),
    )

    parser.add_argument(
        "--interval",
        type=float,
        default=getenv_float("TICKET_PRODUCER_INTERVAL", 1.0),
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=getenv_int("TICKET_PRODUCER_LIMIT", 0),
    )

    parser.add_argument(
        "--client-count",
        type=int,
        default=getenv_int("TICKET_CLIENT_COUNT", 100),
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    producer = None

    if not args.dry_run:
        producer = Producer(
            {
                "bootstrap.servers": args.bootstrap_servers,
                "client.id": "client-ticket-producer",
                "acks": "all",
                "enable.idempotence": True,
            }
        )

    count = 0

    try:
        while True:
            ticket = build_ticket(args.client_count)
            ticket_json = json.dumps(ticket, ensure_ascii=False)

            if args.dry_run:
                print(ticket_json)
            else:
                producer.produce(
                    topic=args.topic,
                    key=ticket["client_id"],
                    value=ticket_json.encode("utf-8"),
                    on_delivery=delivery_report,
                )
                producer.poll(0)

            count += 1

            if args.limit > 0 and count >= args.limit:
                break

            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\nArrêt demandé par l'utilisateur.")

    finally:
        if producer is not None:
            producer.flush(10)


if __name__ == "__main__":
    main()

