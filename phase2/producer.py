import csv, json, time
from datetime import datetime, timezone
from kafka import KafkaProducer

NUM = {"distance_km", "prep_time_min", "courier_experience_yrs", "delivery_time_min"}

p = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode()
)
rows = list(csv.DictReader(open("/data/orders.csv", encoding="utf-8")))

sent = 0
for r in rows:
    for k in NUM:
        if k in r:
            r[k] = float(r[k]) if r[k] not in ("", None) else None
    r["event_ts"] = datetime.now(timezone.utc).isoformat()
    p.send("food-delivery-orders", r)
    sent += 1
    if sent % 10 == 0:
        p.flush()
        print(f"Sent {sent} events...", flush=True)
        time.sleep(1)
p.flush()
print(f"Done: {sent} events")