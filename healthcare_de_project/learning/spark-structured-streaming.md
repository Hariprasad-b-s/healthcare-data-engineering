# Spark Structured Streaming — consuming Kafka into bronze

The companion to `docker-kafka.md`. That document ends with 712,809 FHIR resource
messages sitting in the `healthcare_fhir_stream` topic. This one covers reading
them back out with Spark Structured Streaming and landing them in the bronze
layer — including the environment bug that made the first six attempts fail, and
how it was actually diagnosed.

Result: `scripts/bronze_notebooks/12_streaming_bronze.ipynb`.

---

## 1. What Structured Streaming actually is

The mental model that matters: **a streaming query is a batch query that keeps
re-running.** Spark treats the stream as an unbounded table that gets new rows
appended, and repeatedly runs your query over just the new rows. Each such run is
a **micro-batch**.

Practical consequences:

- The DataFrame API is the same as batch. `select`, `withColumn`, `from_json` —
  all identical. You mostly write normal Spark code.
- `spark.readStream` instead of `spark.read`, `writeStream` instead of `write`.
- You cannot call `.count()` or `.show()` on a streaming DataFrame — there is no
  "all the data" to count. Output goes to a sink, and you inspect it afterwards.
- Two concepts have no batch equivalent: **checkpoints** and **triggers**
  (sections 5 and 6).

---

## 2. The dependency problem — and the trap underneath it

### The Kafka connector is not bundled with Spark

PySpark ships ~286 jars. None of them are the Kafka connector. It must be added
**when the SparkSession is created** — you cannot add jars to a session that is
already running.

The commonly-shown way is Maven coordinates:

```python
.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1")
```

This makes Spark shell out to Ivy, download the jar and everything it transitively
depends on, and put all of it on the classpath. Convenient, but it pulled Hadoop
jars into our classpath too, which makes the result harder to reason about.

We used explicit jars instead — four files, vendored into `healthcare_de_project/jars/`:

```python
kafka_jars = ",".join(sorted(glob.glob("../../jars/*.jar")))
.config("spark.jars", kafka_jars)
```

| Jar | Why |
| --- | --- |
| `spark-sql-kafka-0-10_2.13-4.0.1.jar` | the connector itself |
| `spark-token-provider-kafka-0-10_2.13-4.0.1.jar` | its auth companion, always required |
| `kafka-clients-3.9.1.jar` | the actual Kafka client library |
| `commons-pool2-2.12.1.jar` | connection pooling the connector needs |

Four pinned files reproduce identically on any machine. No network needed after
the first download, and no surprise transitive versions.

### The trap: `SPARK_HOME` silently overrides your pyspark version

This is the single most valuable thing in this document.

```bash
$ python -c "import pyspark; print(pyspark.__version__)"
4.1.1

$ echo $SPARK_HOME
/Users/hariprasad/Downloads/spark/spark-4.0.1-bin-hadoop3
```

**When `SPARK_HOME` is set, PySpark uses the jars from that directory and ignores
the ones bundled inside the Python package.** The Python API says 4.1.1. The engine
that actually runs your job is 4.0.1.

So `pyspark.__version__` is *not* a reliable answer to "what version of Spark am I
running." The reliable answer is:

```python
print(spark.version)          # reports the JVM engine's version - the one that matters
```

Every Kafka connector version we tried against this failed, because they were
matched to 4.1.1 rather than to the real 4.0.1 engine.

### How the failure actually presented

```
java.lang.NoSuchFieldError: TASK_ATTEMPT_ID
```

`NoSuchFieldError` (as opposed to `ClassNotFoundException`) means: the class was
found, but a field the calling code was compiled to expect is not in it. That is
almost always a **version mismatch** — code compiled against version X running
against version Y.

Here, `TASK_ATTEMPT_ID` is a field of Spark's internal `LogKeys` class. It exists
in Spark 4.1.x. It does not exist in 4.0.1. The 4.1.1 connector called it; the
4.0.1 engine did not have it.

**The rule: the Kafka connector version must match `spark.version` exactly** — not
`pyspark.__version__`, not whatever the newest release is.

### Two things that made this harder to see

**py4j swallowed the real error.** The first failure surfaced as:

```
py4j.protocol.Py4JError: An error occurred while calling o44.getCause
```

which says nothing. Spark 4.1.1 has a cosmetic bug where an error-formatting class
(`BreakingChangeInfo`) fails to load, which broke py4j's attempt to extract the
underlying cause. The fix was to stop filtering the output and read the raw Java
stack trace, where `NoSuchFieldError` was plainly visible.

**Lesson:** when a Python-side Spark error is uninformative, the real message is in
the JVM stack trace. Grep the full stderr for `Caused by:` rather than trusting the
Python exception.

**A wrong hypothesis cost two attempts.** `NoSuchFieldError` *is* commonly caused by
Hadoop jar conflicts, and the Ivy cache did contain three different
`hadoop-client-api` versions — so "Ivy is shadowing Hadoop" looked right. Vendoring
explicit jars to eliminate Ivy changed nothing, which disproved it. Only then did
checking the environment reveal `SPARK_HOME`.

**Lesson:** when a plausible fix changes nothing, the hypothesis is wrong — go back
and verify assumptions rather than refining the fix. "What version am I actually
running?" should have been the *first* question, not the sixth.

---

## 3. Reading from Kafka

```python
kafka_df = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "healthcare_fhir_stream")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 50000)
    .load())
```

Kafka's source schema is fixed, and is the same for every topic:

```
key            binary      <- our patient id, needs CAST to string
value          binary      <- our JSON payload, needs CAST to string
topic          string
partition      integer
offset         long
timestamp      timestamp   <- when the broker received it
timestampType  integer
```

`key` and `value` are **binary** — Kafka has no idea what is inside them. Casting
to string is always step one.

| Option | Meaning |
| --- | --- |
| `subscribe` | topic name. Also `subscribePattern` for a regex, `assign` for specific partitions |
| `startingOffsets` | `earliest`, `latest`, or explicit JSON offsets. **Only applies on the first run** — after that the checkpoint decides, and this option is ignored |
| `maxOffsetsPerTrigger` | cap on rows per micro-batch. Without it, the first batch tries to swallow all 712,809 messages at once |

### Gotcha: explicit offsets must list every partition

While testing with a batch read, this failed:

```python
.option("startingOffsets", '{"healthcare_fhir_stream":{"0":0}}')
```

```
KafkaIllegalStateException: Partitions specified for Kafka start offsets
don't match what are assigned
```

The topic has 3 partitions. If you specify offsets explicitly you must specify
**all** of them — a partial map is an error, not a filter:

```python
.option("startingOffsets", '{"healthcare_fhir_stream":{"0":0,"1":0,"2":0}}')
```

### Tip: develop with `spark.read`, not `readStream`

A batch read of a bounded offset range behaves identically to the stream but lets
you call `.show()` and `.collect()`. Every schema decision here was worked out that
way first, then switched to `readStream` once correct.

---

## 4. Parsing — and why the schema is deliberately loose

The producer sends this JSON envelope per message:

```json
{
  "bundle_resource_type": "Bundle",
  "bundle_type": "transaction",
  "fullUrl": "urn:uuid:...",
  "resource_type": "Encounter",
  "resource": { ...full FHIR resource... },
  "request": {"method": "POST", "url": "Encounter"},
  "source_file_name": "Julio255_Stokes453_....json"
}
```

`from_json` needs an explicit schema — Spark will not infer one on a stream,
because inferring requires reading data that has not arrived yet.

```python
message_schema = StructType([
    StructField("bundle_resource_type", StringType()),
    StructField("bundle_type",          StringType()),
    StructField("fullUrl",              StringType()),
    StructField("resource_type",        StringType()),
    StructField("resource",             MapType(StringType(), StringType())),
    StructField("request",              MapType(StringType(), StringType())),
    StructField("source_file_name",     StringType()),
])
```

**Why `resource` is `MapType(String, String)` and not a real struct:** there are 20
different FHIR resource types in this data, and a `Patient` has nothing structurally
in common with an `Observation`. There is no single struct that fits them all.

`MapType(String, String)` keeps every top-level field addressable by name, and
serialises nested objects and arrays to raw JSON strings for silver to parse later:

```python
{'gender': 'male',
 'identifier': '[{"system":"https://github.com/synthetic...',   # nested -> JSON string
 'address':    '[{"extension":[{"url":"http://hl7.org/fh...'}
```

This is the same choice the batch bronze notebook makes — deliberately, so both
paths produce the same schema.

`resource_type` comes from `request.url` rather than `resource.resourceType`, again
matching batch: it is a top-level string, so no nested parsing is needed.

### Schema parity with batch is the whole point

Verified against the existing batch parquet — both produce these eight columns
identically:

```
bundle_resource_type, bundle_type, fullUrl, resource (map),
request (map), input_file_name, resource_type, ingestion_timestamp
```

Streaming adds three more for debugging and replay: `kafka_partition`,
`kafka_offset`, `kafka_timestamp`.

Note `source_file_name` is aliased to `input_file_name` to match batch. Batch stores
a full path, streaming stores a bare filename — same meaning, slightly different
content, worth remembering downstream.

---

## 5. Checkpoints

```python
.option("checkpointLocation", "../../data_lake/_checkpoints/streaming_bronze/")
```

The checkpoint directory is where Spark records **which Kafka offsets it has
already processed**. It is what makes a streaming job restartable.

- Restart the job → it reads the checkpoint and resumes from the last committed
  offset. It does **not** reprocess.
- Delete the checkpoint → the job re-reads from `startingOffsets` and reprocesses
  everything.

This is why re-running the notebook now processes **zero** rows: all 712,809 offsets
are committed. That is the checkpoint working, not a bug.

Two things to know:

- **The checkpoint is tied to the query, not the topic.** Spark's own offset
  tracking replaces Kafka's usual consumer-group mechanism, which is why you do not
  set `group.id` yourself.
- **Do not share one checkpoint between two different queries.** It stores the query
  plan as well as offsets, and reusing it after changing the transformation causes
  errors or silently wrong resumption. New query → new checkpoint directory.

---

## 6. Triggers — how often a micro-batch runs

```python
.trigger(availableNow=True)
```

| Trigger | Behaviour | Use |
| --- | --- | --- |
| `availableNow=True` | Process everything currently in the topic, then **stop** | Draining a backlog; development. What we used |
| `processingTime="30 seconds"` | Run a micro-batch every 30s, forever | Live demo, production |
| *(default, none)* | Micro-batch as fast as possible, forever | Rarely what you want |
| `once=True` | One single micro-batch, then stop | Deprecated, superseded by `availableNow` |

`availableNow` is dramatically easier to develop against: the query terminates, so
`awaitTermination()` returns and the next notebook cell runs. With a continuous
trigger the cell blocks forever and you must interrupt it manually.

Swapping to `processingTime` is the only change needed to make this an always-on
pipeline.

---

## 7. Writing

```python
query = (bronze_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", bronze_streaming_path)
    .option("checkpointLocation", checkpoint_path)
    .partitionBy("resource_type")
    .trigger(availableNow=True)
    .start())

query.awaitTermination()
```

- `outputMode("append")` — only new rows are written, never updates to old ones.
  The only mode file sinks support. (`complete` and `update` exist for aggregations.)
- `partitionBy("resource_type")` — mirrors batch bronze, so silver reads work
  identically against either path.
- `.start()` returns immediately; the query runs in the background.
  `awaitTermination()` blocks until it finishes.

Output goes to `data_lake/bronze/streaming_data/` — deliberately **separate** from
`data_lake/bronze/batch_data/`. Batch and streaming bronze stay physically apart and
are reconciled further downstream.

---

## 8. Verifying the run

```
total rows: 712809
```

The producer reported `712809 resource messages sent`. Exact match — nothing lost,
nothing duplicated.

Counts by resource type carry their own correctness signals:

```
Observation          274,372
Procedure             89,778
DiagnosticReport      64,472
ExplanationOfBenefit  58,904  ┐ identical: every Claim has exactly one EOB
Claim                 58,904  ┘
DocumentReference     33,588  ┐ identical: one clinical note per Encounter
Encounter             33,588  ┘
MedicationRequest     25,316
Condition             20,153
...
Provenance               574  ┐ exactly one per bundle file, and 574 is the
Patient                  574  ┘ bundle file count
```

**`Patient = Provenance = 574 = the source file count`** is the strongest check
available: exactly one patient and one provenance record per source file. If rows
had been dropped or double-counted, this identity would break.

Worth internalising: look for **invariants in the data** (one patient per bundle, one
EOB per claim) rather than only checking a total. A total can be right by accident;
several independent identities holding simultaneously cannot.

---

## 9. Debugging checklist for next time

1. **`print(spark.version)`** — not `pyspark.__version__`. Check `SPARK_HOME` too.
2. **Read the raw JVM stack trace.** Grep full stderr for `Caused by:`. Do not trust
   the Python-side exception; it may be hiding the cause.
3. **`NoSuchFieldError` / `NoSuchMethodError` = version mismatch**, essentially always.
   `ClassNotFoundException` = a jar is missing entirely. Different problems.
4. **Prototype with `spark.read`, not `readStream`.** You get `.show()` back.
5. **If a plausible fix changes nothing, the hypothesis is wrong.** Re-verify
   assumptions instead of refining the fix.

---

## 10. Known issue: the version mismatch is still there

`pyspark` 4.1.1 (Python) against Spark 4.0.1 (engine) is an unsupported combination.
It currently works for both the batch notebooks and this streaming job, but it is the
direct cause of the debugging session above and will resurface with the next add-on
library — **Delta Lake especially**, since Delta versions are tightly coupled to Spark
versions.

Two ways to resolve it:

```bash
# A: use the standalone Spark 4.0.1 install, match the Python package to it
pip install pyspark==4.0.1

# B: drop the standalone install, let the Python package supply its own 4.1.1 jars
unset SPARK_HOME    # and remove it from ~/.zshrc
```

Either is fine; they must simply agree. Worth settling **before** starting the Delta
Lake work.

---

## 11. What's next

- **Delta Lake for the gold layer**, so batch and streaming can both write into the
  same gold tables via `MERGE INTO` instead of the batch notebooks' current
  `.mode("overwrite")` full rewrite.
- **Streaming silver transformations**, so streaming bronze flows through to the same
  dimensional model as batch.
- **Airflow** to orchestrate the batch DAG once the streaming path is complete.
