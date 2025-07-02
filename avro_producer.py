# pip install avro-python3
# pip install google-cloud-pubsub

import io
import time
import json
import uuid
import random
import datetime
import avro.schema
from avro.io import DatumWriter, BinaryEncoder
from google.pubsub_v1.types.schema import Schema, Encoding, GetSchemaRequest, SchemaView
from google.pubsub_v1.services.publisher import PublisherClient
from google.pubsub_v1.services.schema_service import SchemaServiceClient
from google.pubsub_v1.types.pubsub import PubsubMessage, PublishRequest

PROJECT_ID      = "euphoric-fusion-462011-r4"
TOPIC_ID        = "travel"

def fetch_topic_schema_and_encoding(project_id: str, topic_id: str):
    publisher    = PublisherClient()
    topic_path   = publisher.topic_path(project_id, topic_id)
    topic        = publisher.get_topic(request={"topic": topic_path})
    schema_name  = topic.schema_settings.schema
    # schema_name  = "projects/mythic-aloe-457912-d5/schemas/travel-event-schema-new"
    encoding     = topic.schema_settings.encoding
    # encoding     = "Binary"

    schema_client = SchemaServiceClient()
    req = GetSchemaRequest(
        name=schema_name,
        view=SchemaView.FULL
    )
    schema_obj   = schema_client.get_schema(request=req)
    avro_schema  = avro.schema.parse(schema_obj.definition)
    return avro_schema, encoding, topic_path

def mock_booking() -> dict:
    today    = datetime.date.today()
    checkin  = today + datetime.timedelta(days=random.randint(1,30))
    checkout = checkin + datetime.timedelta(days=random.randint(1,7))
    return {
      "booking_id":    str(uuid.uuid4()),
      "user_id":       f"user-{random.randint(1000,9999)}",
      "hotel_id":      f"hotel-{random.randint(100,199)}",
      "booking_date":  today.isoformat(),
      "checkin_date":  checkin.isoformat(),
      "checkout_date": checkout.isoformat(),
      "room_type":     random.choice(["SINGLE","DOUBLE","SUITE"]),
      "amount":        round(random.uniform(80,500),2),
      "currency":      "USD",
      "status":        random.choice(["CONFIRMED","CANCELLED","PENDING"])
    }

def serialize_record(avro_schema, record: dict) -> bytes:
    buf     = io.BytesIO()
    encoder = BinaryEncoder(buf)
    writer  = DatumWriter(avro_schema)
    writer.write(record, encoder)
    return buf.getvalue()

def main():
    avro_schema, encoding, topic_path = fetch_topic_schema_and_encoding(
        PROJECT_ID, TOPIC_ID
    )
    publisher = PublisherClient()

    for _ in range(100):
        rec = mock_booking()
        if encoding == Encoding.BINARY:
            data = serialize_record(avro_schema, rec)
            # data = "it will be byte version of message (serialized)" 
        elif encoding == Encoding.JSON:
            data = json.dumps(rec).encode("utf-8")
        else:
            raise RuntimeError(f"Unsupported encoding: {encoding}")

        # Build a single-message PublishRequest
        msg = PubsubMessage(data=data)
        req = PublishRequest(topic=topic_path, messages=[msg])

        resp = publisher.publish(request=req)
        # resp.message_ids is a list, one ID per message published
        print(f"Published booking_id={rec['booking_id']} as msg_id={resp.message_ids[0]}")

        time.sleep(2)

if __name__ == "__main__":
    main()