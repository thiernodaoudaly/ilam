"""
ILAM — Kafka Producer
Simulates real-time CDR voice and network events streaming into Kafka.
"""

import json
import random
import uuid
import time
from datetime import datetime, timedelta, date
from faker import Faker
from kafka import KafkaProducer

fake = Faker('fr_FR')
random.seed()

KAFKA_BOOTSTRAP = "localhost:9094"
TOPIC_CDR_VOICE = "cdr_voice_stream"
TOPIC_NETWORK = "network_events_stream"

REGIONS = [
    "Dakar", "Thiès", "Saint-Louis", "Ziguinchor",
    "Kaolack", "Tambacounda", "Louga", "Fatick",
    "Kolda", "Matam", "Kaffrine", "Kédougou",
    "Sédhiou", "Diourbel"
]
TECHNOLOGIES   = ["2G", "3G", "4G", "4G+", "5G"]
TECH_WEIGHTS   = [0.05, 0.15, 0.45, 0.30, 0.05]
OFFERS_PREPAID = ["YOONU_1", "YOONU_5", "YOONU_10", "ILLIMIX_500"]
SEVERITIES     = ["CRITICAL", "MAJOR", "MINOR", "WARNING", "INFO"]
EVENT_TYPES    = ["ALARM", "PERFORMANCE", "CONGESTION", "OUTAGE", "RECOVERY"]

def msisdn():
    return "+221{}{}".format(
        random.choice(["77", "78", "70", "76", "75"]),
        random.randint(1000000, 9999999)
    )

def gen_cdr_voice():
    now = datetime.now()
    dur = random.randint(5, 1800)
    return {
        "cdr_id":          str(uuid.uuid4()),
        "calling_msisdn":  msisdn(),
        "called_msisdn":   msisdn(),
        "call_direction":  random.choice(["MO", "MT"]),
        "call_type":       random.choice(["ON_NET", "OFF_NET", "INTERNATIONAL"]),
        "duration_seconds": dur,
        "call_status":     random.choice(["SUCCESS", "SUCCESS", "SUCCESS", "FAILED"]),
        "disconnect_cause": random.choice(["NORMAL", "BUSY", "NO_ANSWER"]),
        "origin_cell_id":  "CELL_{}".format(random.randint(1000, 9999)),
        "origin_region":   random.choice(REGIONS),
        "destination_type": random.choice(["MOBILE", "FIXE", "INTERNATIONAL"]),
        "roaming_flag":    random.random() < 0.05,
        "roaming_country": random.choice(["FR", "CI", "ML", "GN"]),
        "charge_amount":   round(dur * random.uniform(0.001, 0.005), 2),
        "currency":        "XOF",
        "offer_code":      random.choice(OFFERS_PREPAID),
        "start_ts":        now.isoformat(),
        "end_ts":          (now + timedelta(seconds=dur)).isoformat(),
        "ingested_at":     now.isoformat(),
        "call_date":       str(date.today()),
    }

def gen_network_event():
    now = datetime.now()
    threshold = random.uniform(70, 99)
    value = random.uniform(50, 105)
    return {
        "event_id":        str(uuid.uuid4()),
        "event_type":      random.choice(EVENT_TYPES),
        "severity":        random.choice(SEVERITIES),
        "cell_id":         "CELL_{}".format(random.randint(1000, 9999)),
        "site_id":         "SITE_{}".format(random.randint(100, 999)),
        "region":          random.choice(REGIONS),
        "technology":      random.choices(TECHNOLOGIES, TECH_WEIGHTS)[0],
        "metric_name":     random.choice(["availability", "drop_rate", "congestion", "throughput"]),
        "metric_value":    round(value, 4),
        "threshold_value": round(threshold, 4),
        "vendor":          random.choice(["HUAWEI", "ERICSSON", "NOKIA", "ZTE"]),
        "raw_payload":     "{}",
        "source_system":   random.choice(["OSS", "NMS", "EMS"]),
        "event_ts":        now.isoformat(),
        "ingested_at":     now.isoformat(),
        "event_date":      str(date.today()),
    }

def main():
    print("=" * 60)
    print("ILAM — Kafka Producer")
    print("Streaming CDR voice + Network Events")
    print("=" * 60)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=3,
    )

    print("Connected to Kafka. Starting stream...")
    print("Press Ctrl+C to stop.\n")

    count_cdr = 0
    count_net = 0

    try:
        while True:
            # Publier 3 CDR voix
            for _ in range(3):
                cdr = gen_cdr_voice()
                producer.send(
                    TOPIC_CDR_VOICE,
                    key=cdr["calling_msisdn"],
                    value=cdr
                )
                count_cdr += 1

            # Publier 1 événement réseau
            event = gen_network_event()
            producer.send(
                TOPIC_NETWORK,
                key=event["cell_id"],
                value=event
            )
            count_net += 1

            producer.flush()
            print("Sent: {} CDR voice | {} network events".format(
                count_cdr, count_net), end="\r")

            time.sleep(1)

    except KeyboardInterrupt:
        print("\n\nStopped by user.")
        print("Total sent: {} CDR voice, {} network events".format(
            count_cdr, count_net))
        producer.close()

if __name__ == "__main__":
    main()
