import argparse
import json
import os
import random
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError

TOPIC_NAME = "healthcare_fhir_stream"
BOOTSTRAP_SERVERS = "localhost:9092"
# Anchor to this file's location, not the caller's working directory, so the
# script runs the same from anywhere (shell, notebook, scheduler).
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
STREAMING_SOURCE_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "streaming_source"))
MAX_REQUEST_SIZE = 5 * 1024 * 1024  # matches the topic's max.message.bytes


def list_bundle_files(streaming_dir):
    folders = sorted(f for f in os.listdir(streaming_dir) if f.startswith("folder_"))
    files = []
    for folder in folders:
        folder_path = os.path.join(streaming_dir, folder)
        for file_name in os.listdir(folder_path):
            if file_name.endswith(".json"):
                files.append(os.path.join(folder_path, file_name))
    return files


def bundle_patient_id(bundle, fallback_key):
    try:
        first_entry = bundle["entry"][0]
        resource = first_entry.get("resource", {})
        if resource.get("resourceType") == "Patient" and "id" in resource:
            return resource["id"]
    except (KeyError, IndexError, TypeError):
        pass
    return fallback_key


def entry_messages(bundle, file_name, patient_key):
    """Explode one FHIR bundle into one message per entry (one FHIR resource
    each). This keeps individual Kafka messages small (KB, not the multi-MB
    size of a whole bundle) and lets the streaming bronze job skip the
    explode step the batch bronze notebook has to do."""
    bundle_resource_type = bundle.get("resourceType")
    bundle_type = bundle.get("type")

    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        request = entry.get("request", {})
        resource_type = request.get("url") or resource.get("resourceType")

        payload = {
            "bundle_resource_type": bundle_resource_type,
            "bundle_type": bundle_type,
            "fullUrl": entry.get("fullUrl"),
            "resource_type": resource_type,
            "resource": resource,
            "request": request,
            "source_file_name": file_name,
        }
        yield patient_key, json.dumps(payload)


def on_send_error(excp):
    print(f"  ! delivery failed: {excp}")


def main():
    parser = argparse.ArgumentParser(description="Stream FHIR bundle entries into Kafka, one message per resource")
    parser.add_argument("--file-rate", type=float, default=2.0, help="bundle files per second to simulate arrival (default: 2). Entries within a file are sent back-to-back.")
    parser.add_argument("--limit", type=int, default=None, help="only process the first N bundle files (for smoke testing)")
    parser.add_argument("--shuffle", action="store_true", help="shuffle file order to simulate arrival from many patients at once")
    parser.add_argument("--topic", default=TOPIC_NAME, help=f"Kafka topic (default: {TOPIC_NAME})")
    args = parser.parse_args()

    files = list_bundle_files(STREAMING_SOURCE_DIR)
    if args.shuffle:
        random.shuffle(files)
    if args.limit:
        files = files[: args.limit]

    if not files:
        print(f"No .json files found under {STREAMING_SOURCE_DIR}")
        return

    print(f"Found {len(files)} bundle files to stream to topic '{args.topic}' at {args.file_rate} files/s")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: v.encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=50,
        max_request_size=MAX_REQUEST_SIZE,
        buffer_memory=64 * 1024 * 1024,
    )

    file_delay = 1.0 / args.file_rate if args.file_rate > 0 else 0
    files_sent = 0
    entries_sent = 0
    try:
        for file_path in files:
            with open(file_path, "r") as f:
                bundle = json.load(f)

            file_name = os.path.basename(file_path)
            patient_key = bundle_patient_id(bundle, fallback_key=file_name)

            for key, value in entry_messages(bundle, file_name, patient_key):
                future = producer.send(args.topic, key=key, value=value)
                future.add_errback(on_send_error)
                entries_sent += 1

            files_sent += 1
            print(f"  [{files_sent}/{len(files)}] {file_name}: {len(bundle.get('entry', []))} resources queued (total entries sent: {entries_sent})")

            if file_delay:
                time.sleep(file_delay)
    except KafkaError as e:
        print(f"Producer error: {e}")
    finally:
        producer.flush()
        producer.close()
        print(f"Done. {files_sent} bundle files / {entries_sent} resource messages sent to '{args.topic}'.")


if __name__ == "__main__":
    main()
