# Kafka on Docker — how we set it up, step by step

This walks through everything we did to get a local Kafka broker running in
Docker and streaming real FHIR bundle data into it: the compose file, why
each setting is there, how to start/inspect/reset the broker, and the
producer script — including the two problems we actually hit along the way
(oversized messages, and throughput) and why we fixed them the way we did.

---

## 1. Kafka concepts, quickly

If you're new to Kafka, the vocabulary you need for this project:

- **Broker** — a Kafka server process. We're running exactly one (`local-kafka`), which is enough for local dev.
- **Topic** — a named stream, e.g. `healthcare_fhir_stream`. Producers write to it, consumers read from it.
- **Partition** — a topic is split into N ordered, independent logs (partitions). Kafka only guarantees message *order within a partition*, not across the whole topic. More partitions = more parallelism for consumers.
- **Offset** — each message's position within its partition (0, 1, 2, ...). Consumers track "how far they've read" by offset.
- **Key** — every message has an optional key + value. Kafka hashes the key to decide which partition a message goes to. Same key → always same partition → order preserved for that key. We key every message by patient id, so all of one patient's events land on the same partition in order.
- **Replication factor** — how many broker copies of each partition exist. We use `1` because we have one broker; production clusters use `3`.
- **Consumer group** — a set of consumers sharing the work of reading a topic; Kafka assigns each partition to exactly one consumer in the group. Not built yet in this project — that's the upcoming Spark Structured Streaming job.
- **KRaft mode** — Kafka's own Raft-based consensus for cluster metadata. It replaced ZooKeeper (ZooKeeper is gone entirely as of Kafka 4.x). One process can be both a broker and a metadata "controller" — that's what we're running.

---

## 2. Why `confluentinc/cp-kafka` and not a "generic" Kafka image?

Worth answering up front, because the compose file below pins a *vendor's*
image rather than something from Apache — which looks arbitrary until you know
the history.

### Kafka is just a Java app with a properties file

Installed the old-fashioned way, Kafka is: download a `.tgz` from apache.org,
unpack it, edit `config/server.properties`, run
`bin/kafka-server-start.sh config/server.properties`. That properties file is
Kafka's **only** native configuration mechanism:

```properties
node.id=1
listeners=PLAINTEXT://0.0.0.0:9092
advertised.listeners=PLAINTEXT://localhost:9092
process.roles=broker,controller
```

A Docker image is just that whole installation pre-packaged and frozen. But
*someone* has to build it — pick a base OS, install Java, drop in the tarball,
write a startup script. For most of Kafka's life, **Apache published the source
and the tarball but no Docker image.** Confluent — the company founded by the
engineers who created Kafka at LinkedIn — did publish one, and it became the de
facto standard purely by being the well-maintained option that existed.

That's the single biggest practical reason to use it while learning: the
overwhelming majority of Kafka-in-Docker tutorials, blog posts and
StackOverflow answers use `confluentinc/cp-kafka`, so **an error message you
paste into a search engine returns an answer matching your exact setup.**

### What the image concretely does for you

**1. Environment variables instead of a mounted config file.** Kafka itself has
no idea what an environment variable is. Confluent's image contains a startup
script that mechanically translates them before Kafka boots. The rule is simply
*strip the `KAFKA_` prefix, lowercase, turn `_` into `.`*:

| Compose env var                                     | Becomes in `server.properties`                |
| --------------------------------------------------- | ----------------------------------------------- |
| `KAFKA_NODE_ID: 1`                                | `node.id=1`                                   |
| `KAFKA_PROCESS_ROLES: broker,controller`          | `process.roles=broker,controller`             |
| `KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://...`     | `advertised.listeners=PLAINTEXT://...`        |

So the entire broker config lives in the compose file as readable YAML, instead
of maintaining a separate `server.properties` on disk and mounting it as a
volume. One file to read, one file to change.

**2. Automatic KRaft storage formatting.** In KRaft mode, before a broker can
start for the very first time its storage directory must be *formatted* with a
cluster ID — a genuinely separate command
(`kafka-storage.sh format --cluster-id ... --config server.properties`).
Confluent's image detects unformatted storage and runs that step for you using
the `CLUSTER_ID` you supply. Without it, the container would boot, fail with an
"unformatted log directory" error, and you'd have to work out the two-phase
startup yourself.

**3. Ecosystem consistency.** If Schema Registry, Kafka Connect or ksqlDB get
added later, Confluent's sibling images (`cp-schema-registry`,
`cp-kafka-connect`, ...) share the same versioning scheme and env-var
conventions.

### The alternative, honestly

Since **Kafka 3.7 (early 2024)** Apache does publish an official image:
`apache/kafka`. It's legitimate and lighter.

|                                        | `confluentinc/cp-kafka`         | `apache/kafka`               |
| -------------------------------------- | --------------------------------- | ------------------------------ |
| Tutorials / answers online             | Enormous amount                   | Growing, much less             |
| Image size                             | Heavier (~600MB+)                 | Lighter (~350MB)               |
| Confluent-specific tooling bundled     | Yes (unused here)                 | No                             |
| Adding Schema Registry / Connect later | Drop-in siblings                  | Mixing vendors, more friction  |

**Decision for this project: stay on Confluent.** The image choice changes
nothing about how Kafka *behaves* or what you learn — topics, partitions,
offsets, consumer groups and producers are identical either way. The 250MB is
worth the searchability while learning.

---

## 3. The `docker-compose.yml`

```yaml
services:
  kafka:
    image: confluentinc/cp-kafka:7.4.0   # Confluent's packaged build of Apache Kafka (~3.4/3.5 core)
    container_name: local-kafka
    ports:
      - "9092:9092"                      # host:container — lets processes on the Mac reach Kafka at localhost:9092
    environment:
      KAFKA_NODE_ID: 1
      CLUSTER_ID: 'MkU3OEVBNTcwNTJENDM2Qk'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER'
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@kafka:9093'
      KAFKA_PROCESS_ROLES: 'broker,controller'
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
```

This is a **single-broker, KRaft-mode Kafka cluster** (no ZooKeeper), reachable
from the host machine at `localhost:9092`.

| Variable                                                                | What it does                                                                                                                                                                                                                                                                                    |
| ----------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `KAFKA_NODE_ID: 1`                                                    | This node's ID within the cluster (single node, so just`1`)                                                                                                                                                                                                                                   |
| `CLUSTER_ID`                                                          | A random 16-byte identifier for the cluster (base64-encoded), stamped into on-disk storage the first time the broker starts. **Not a mode or feature switch** — see the detailed note below.                                                                                                     |
| `KAFKA_PROCESS_ROLES: 'broker,controller'`                            | In KRaft, a node can be a**broker** (serves producer/consumer traffic), a **controller** (manages cluster metadata via Raft), or both — combined here since it's a single-node dev setup                                                                                           |
| `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP`                                | Declares two named listeners, both PLAINTEXT (no TLS/auth):`CONTROLLER` for Raft traffic between controller nodes, `PLAINTEXT` for regular client traffic                                                                                                                                   |
| `KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093` | What the broker actually binds to **inside the container** — client port 9092, controller/Raft port 9093                                                                                                                                                                                |
| `KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092`              | The address the broker hands back to clients in metadata responses ("here's how to reach me"). Because it says`localhost`, this only works cleanly for clients running on the host itself — which is exactly our case (Spark + the Python producer run on the Mac, not in another container) |
| `KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER'`                       | Tells Kafka which listener name carries Raft/controller traffic                                                                                                                                                                                                                                 |
| `KAFKA_CONTROLLER_QUORUM_VOTERS: '1@kafka:9093'`                      | The KRaft quorum membership: node`1` reachable at `kafka:9093` — the Compose service's internal DNS name, since this is broker-to-broker traffic inside the Docker network, not client traffic                                                                                             |
| `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1`                           | Replication for the internal`__consumer_offsets` topic — `1` is fine for a single broker, you'd want `3` in a real cluster                                                                                                                                                               |
| `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0`                           | Kafka normally waits a few seconds after the first consumer joins a group, to batch in other joiners before rebalancing.`0` skips that wait — good for dev, you'd raise it in prod to avoid rebalance storms                                                                                 |
| `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR` / `..._REPLICATION_FACTOR: 1` | Same replication story, but for the internal`__transaction_state` topic used by transactional/exactly-once producers                                                                                                                                                                          |

### The `CLUSTER_ID`, in detail

`CLUSTER_ID: 'MkU3OEVBNTcwNTJENDM2Qk'` looks cryptic and deliberate. It is
neither. **It does not select a mode, enable a feature, or mean anything.**
It's a name tag.

Kafka's only requirement is: **exactly 16 bytes, base64-encoded** (a
22-character string). The content is meant to be random and meaningless.
Decoding the one in this file:

```python
import base64
base64.urlsafe_b64decode('MkU3OEVBNTcwNTJENDM2Qk' + '==')
# b'2E78EA57052D436B'   → 16 bytes
```

Whoever originally created it base64-encoded the *ASCII text* of a hex string
rather than raw UUID bytes — a slightly odd way to do it, but it satisfies the
16-byte rule so Kafka accepts it. This exact value appears in Confluent's own
quickstart documentation, which is why it's been copy-pasted into thousands of
tutorials (and why any AI-generated compose file reaches for it reflexively).

A "properly" generated one uses random UUID bytes:

```bash
# canonical way, using Kafka's own tool inside the running container
docker exec local-kafka kafka-storage random-uuid

# equivalent, without a running broker
python3 -c "import base64,uuid; print(base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip('='))"
# e.g. Rgq6Z3DGRAa0TsuaoSHBnw
```

Both are equally valid.

**So why does it exist at all?** It's a safety interlock — think of it as a
serial number stamped into the data directory. On first start, Kafka formats
its log directory and writes the ID into a `meta.properties` file there. On
every later startup it compares the configured ID against the stored one, and
refuses to start if they differ:

```
InconsistentClusterIdException: The Cluster ID <X> doesn't match
stored clusterId <Y> in meta.properties
```

That prevents a genuinely nasty accident: a broker still holding *cluster A's*
data being misconfigured into *cluster B*, where it would start serving stale
foreign partition data as though it were legitimate. The ID makes that
impossible to do silently.

**Does it matter here?** Practically no — *because* there's no `volumes:` entry
(see below). Storage is destroyed on every `docker compose down`, so the ID is
freshly stamped each time and never has an old value to conflict with. It could
be changed to any valid 16-byte string with no effect.

It starts mattering the moment a volume is added to make Kafka data survive
restarts:

```yaml
    volumes:
      - kafka-data:/var/lib/kafka/data
```

From then on the ID is locked in — change it and the broker won't start until
the volume is also wiped.

### `KAFKA_PROCESS_ROLES` — *this* is the actual mode switch

Easy to conflate with `CLUSTER_ID`, but the mode lives entirely in this one
variable:

| Value                 | Meaning                                                                        |
| --------------------- | ------------------------------------------------------------------------------ |
| `broker`            | Node only stores and serves data                                               |
| `controller`        | Node only manages cluster metadata (who's alive, who owns which partition)     |
| `broker,controller` | **Both — "combined mode."** Standard for a single-node dev setup like this one |

In production these are separated onto different machines. Here, one container
does both jobs.

### Storage and networking notes

**No `volumes:` entry** — topic data lives only in the container's writable
layer. `docker compose down` (not just `stop`) wipes it. Fine for a dev
pipeline; worth knowing before you wonder where your test messages went.

**The #1 gotcha with any Kafka-in-Docker setup**: `advertised.listeners` must
be an address reachable from wherever the *client* actually runs. Client on
the host → `localhost:PORT` (needs the `ports:` mapping, what we have).
Client running in another container on the same Compose network → the
service name instead, e.g. `kafka:29092` (would need a second, internal-only
listener). Get this wrong and you get confusing "connection refused" or
timeout errors even though the broker is clearly running.

---

## 4. Starting and inspecting the broker

```bash
# from healthcare_de_project/, where docker-compose.yml lives
docker compose up -d
docker compose ps
docker compose logs kafka --tail 30
```

Look for `Kafka Server started` in the logs — that means the broker finished
KRaft startup and is accepting connections.

To check the daemon is even running before any of this:

```bash
docker info >/dev/null 2>&1 && echo "docker daemon: running" || echo "docker daemon: NOT running"
```

To stop it (keeps the container, just not running):

```bash
docker compose stop
```

To tear it down entirely (removes the container **and its topic data**,
since there's no volume):

```bash
docker compose down
```

---

## 5. Creating a topic

Confluent's image ships the Kafka CLI tools inside the container, so we
exec into it rather than installing a separate Kafka client locally:

```bash
docker exec local-kafka kafka-topics --create \
  --topic healthcare_fhir_stream \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1 \
  --if-not-exists
```

- `--partitions 3` — three independent logs for this topic, so up to 3
  consumers in a group can read it in parallel later.
- `--replication-factor 1` — only one broker, so no redundancy is possible;
  would be `3` in a real cluster.
- `--if-not-exists` — makes the command idempotent, safe to re-run.

Useful follow-ups:

```bash
docker exec local-kafka kafka-topics --list --bootstrap-server localhost:9092
docker exec local-kafka kafka-topics --describe --topic healthcare_fhir_stream --bootstrap-server localhost:9092
```

`--describe` shows per-partition leader/replica/in-sync-replica (ISR) info —
useful for confirming the topic actually came up healthy.

### Resetting a topic during development

While iterating on the producer, it's convenient to wipe a topic back to
empty rather than accumulate test messages:

```bash
docker exec local-kafka kafka-topics --delete --topic healthcare_fhir_stream --bootstrap-server localhost:9092
docker exec local-kafka kafka-topics --create --topic healthcare_fhir_stream --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec local-kafka kafka-configs --alter --topic healthcare_fhir_stream --bootstrap-server localhost:9092 --add-config max.message.bytes=5242880
```

Note the last step: topic-level config overrides (like `max.message.bytes`
below) don't survive a delete+recreate — they have to be reapplied.

---

## 6. Building the producer — and the two problems we hit

The goal: read the FHIR bundle JSON files sitting in
`streaming_source/folder_26` through `folder_50` (574 files — these are the
25 folders set aside for "streaming" data, mirroring the 25 folders used for
batch) and publish them to Kafka so a Structured Streaming job can pick them
up later.

### Problem 1: whole bundles are too big for a single Kafka message

The first version of the producer read each file whole and sent it as one
Kafka message. Testing that against real data:

```
find streaming_source -iname "*.json" -exec stat -f "%z" {} \; | ...
count: 574
min: 117704   max: 42864339   median: 2328864   mean: 3816857
```

Files run from ~115KB up to **43MB**, median ~2.3MB. Kafka's defaults are
`message.max.bytes` (broker/topic side) and `max.request.size` (producer
side), both **1MB**. Sending these files as single messages failed
immediately:

```
MessageSizeTooLargeError: The message is 3383724 bytes when serialized
which is larger than the maximum request size you have configured...
```

The tempting fix is to just raise those limits to ~50MB everywhere. That
works, but it's not good Kafka practice — Kafka is optimized for many small
messages, not a few huge ones. Oversized messages hurt broker memory
pressure, replication latency, and consumer fetch behavior, and every
downstream consumer (including the Spark job we haven't built yet) would
also need matching oversized-fetch configs.

**The better fix: explode each bundle before it reaches Kafka.** A FHIR
bundle file is just a JSON array of `entry` objects, each one a single FHIR
resource (a `Patient`, an `Encounter`, a `Claim`, ...) — this is exactly the
same "entry" structure the *batch* bronze notebook explodes in Spark after
reading the whole file. We just do that explosion earlier, in the producer,
and send **one Kafka message per FHIR resource** instead of one per file.

Checking the size of individual resources after exploding:

```
top 5 entry sizes in the largest files:
 [('Provenance', 1003555), ('ExplanationOfBenefit', 62066), ('ExplanationOfBenefit', 55485), ...]
```

Almost everything drops to a few KB. There's one outlier per bundle — a
`Provenance` resource that Synthea generates listing every other resource in
the bundle, which lands right around ~1MB. To comfortably clear that with
margin, we set both the topic and the producer to a **5MB** limit instead of
guessing exactly at the edge:

```bash
docker exec local-kafka kafka-configs --alter \
  --topic healthcare_fhir_stream --bootstrap-server localhost:9092 \
  --add-config max.message.bytes=5242880    # 5 MiB
```

```python
producer = KafkaProducer(
    ...,
    max_request_size=5 * 1024 * 1024,
    buffer_memory=64 * 1024 * 1024,
)
```

This also simplifies the *downstream* Spark Structured Streaming bronze job:
since messages already arrive pre-exploded with `resource_type` attached,
that job just parses and partitions — it doesn't need to redo the
explode-the-bundle step the batch bronze notebook has to do.

### Problem 2: per-message rate limiting doesn't scale

Exploding each bundle into per-resource messages means a lot more messages —
an average bundle has ~1,167 entries, and there are 574 files, so roughly:

```
574 files × ~1,167 entries/file ≈ 670,000 messages total
```

The first cut of the script throttled with `time.sleep()` **between every
message** to simulate a slow, realistic stream. At even 5 messages/sec, 670K
messages would take **~37 hours** — clearly wrong for a demo/dev pipeline.

The fix: throttle **between bundle files**, not between individual resource
messages. All of one patient's ~1,000 resource messages get sent back-to-back
(a burst), then the script waits before moving to the next patient's bundle.
This is also a more realistic simulation — a patient's full record tends to
arrive/get processed together, not trickled in resource-by-resource.

```python
file_delay = 1.0 / args.file_rate   # e.g. 2 files/sec → 0.5s between files
...
for file_path in files:
    ...                              # send every entry in this bundle, no delay between them
    time.sleep(file_delay)
```

At the default `--file-rate 2.0`, streaming all 574 files takes about
5 minutes.

---

## 7. The finished producer script

`scripts/02_kafka_producer.py`. Key pieces:

- **`list_bundle_files`** — walks `streaming_source/folder_*` and collects every `.json` path.
- **`bundle_patient_id`** — pulls the patient's FHIR id out of the bundle's first entry (Synthea always puts the `Patient` resource first) to use as the Kafka message **key**. Falls back to the filename if that ever fails. Using a stable key means every resource belonging to one patient lands on the same partition, in send order.
- **`entry_messages`** — the explode step: for each `entry` in the bundle, builds a small JSON payload:
  ```json
  {
    "bundle_resource_type": "Bundle",
    "bundle_type": "transaction",
    "fullUrl": "urn:uuid:...",
    "resource_type": "Encounter",
    "resource": { ...the actual FHIR resource... },
    "request": {"method": "POST", "url": "Encounter"},
    "source_file_name": "Earleen680_..._2e649f3e....json"
  }
  ```

  `resource_type` comes from `request.url`, the same field the batch bronze notebook uses (`request.url` and `resource.resourceType` always agree in this data) — that's the field the future streaming bronze job will partition on, just like batch does.
- **Producer config**: `acks="all"` (wait for the write to be durable before considering it sent), `retries=5`, `linger_ms=50` (batch messages briefly for throughput), `max_request_size`/`buffer_memory` sized for the Provenance outlier.
- **CLI flags**:
  - `--file-rate` (default `2.0`) — bundle files per second; controls how "live" the simulated stream feels.
  - `--limit N` — only process the first N files, for smoke testing.
  - `--shuffle` — randomize file order (simulates many different patients' data arriving interleaved, rather than one folder at a time).
  - `--topic` — override the target topic.

### Running it

```bash
cd healthcare_de_project/scripts

# smoke test — 2 files, ~1,800 resource messages
python3 02_kafka_producer.py --limit 2

# full run — all 574 files, ~670K resource messages, ~5 minutes
python3 02_kafka_producer.py
```

### Verifying messages actually landed

Message counts per partition (fast, no need to actually read messages):

```bash
docker exec local-kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 --topic healthcare_fhir_stream
# healthcare_fhir_stream:0:0
# healthcare_fhir_stream:1:0
# healthcare_fhir_stream:2:1790
```

(All messages landing on one partition here was just an artifact of testing
with only 2 distinct patient keys — with the full 574-patient run, hashing
spreads keys across all 3 partitions much more evenly.)

Reading a couple of messages back, keys included:

```bash
docker exec local-kafka kafka-console-consumer \
  --topic healthcare_fhir_stream --bootstrap-server localhost:9092 \
  --from-beginning --max-messages 2 --property print.key=true --partition 2
```

Confirmed: key = patient id, `resource_type` correctly tagged per message,
and the nested `resource` JSON round-trips intact.

---

## 8. Glossary of the size/throughput knobs we touched

| Setting               | Where        | Meaning                                                                                                                                                                                                                                                                              |
| --------------------- | ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `message.max.bytes` | Broker/topic | Largest message the**broker** will accept for a topic                                                                                                                                                                                                                          |
| `max_request_size`  | Producer     | Largest message the**producer** will attempt to send (must be ≤ broker's limit or sends fail client-side before even reaching the broker)                                                                                                                                     |
| `buffer_memory`     | Producer     | Total memory the producer can use to buffer messages awaiting send — needs headroom above`max_request_size` under load                                                                                                                                                            |
| `acks`              | Producer     | Durability tradeoff:`0` = fire and forget, `1` = leader broker wrote it, `all` = all in-sync replicas wrote it (safest; with replication factor 1 here, `all` and `1` behave the same, but `all` is the right habit for when this points at a real multi-broker cluster) |
| `linger_ms`         | Producer     | How long the producer waits to batch up messages before sending, trading a little latency for throughput                                                                                                                                                                             |

---

## 9. What's next

- A **Spark Structured Streaming** notebook that consumes `healthcare_fhir_stream`, parses each message's JSON payload, and writes it out partitioned by `resource_type` — into a **separate streaming bronze path** from the batch bronze data (decision made explicitly: batch and streaming bronze stay physically separate; they get reconciled further downstream).
- Converting the **gold layer to Delta Lake** so batch and streaming can both write into the *same* gold tables via `MERGE INTO`, instead of the batch notebooks' current full `.mode("overwrite")` rewrite (which would clobber anything streaming had written).
