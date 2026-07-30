# RAG Ingestion Framework — Event-Driven Architecture with Apache Airflow (MWAA)
## Orchestration Layer Redesign | AWS-Native | Python

**Change from previous design:** Step Functions → **Amazon MWAA (Managed Workflows for Apache Airflow)**.
Core pipeline stages, repo structure, and file responsibilities (`parsers.py`, `validators.py`, `cleanup.py`, `privacy.py`, `chunker.py`, `metadata.py`, `index_manager.py`) are **unchanged** — they become the task bodies inside an Airflow DAG instead of Lambda handlers behind Step Functions states.

---

## 1. Why Event-Driven + Airflow (vs the earlier Step Functions design)

| Concern | Step Functions | Airflow (MWAA) |
|---|---|---|
| Long-running async waits (Transcribe) | `waitForTaskToken` + EventBridge callback | **Deferrable operators** — task suspends, frees the worker slot, resumes via the Airflow **Triggerer** process on an asyncio event loop — no separate callback Lambda needed |
| Per-file dynamic fan-out | Manual `Map` state | **Dynamic Task Mapping** — one task definition, N mapped instances (one per S3 object in a batch) |
| Cross-file/backfill/reprocessing | Re-invoke state machine manually | Native DAG re-run, backfill, and catch-up scheduling |
| Observability | CloudWatch + Step Functions console | Airflow UI: per-task logs, Gantt view, retry history, SLA misses — better for a data-engineering team already using Airflow |
| You said: "we are using orchestration airflow" | — | This is the fit |

Both are valid; this document assumes **MWAA is the standing orchestrator for your org**, so the ingestion framework should be event-triggered Airflow DAGs, not Step Functions.

---

## 2. Event-Driven Architecture — End to End

```
┌───────────────────────────┐
│   S3 SOURCE BUCKET          │
│  raw/{text,image,audio,     │
│       video}/...            │
└─────────────┬───────────────┘
              │  S3 Event Notification (ObjectCreated:Put)
              ▼
┌───────────────────────────┐
│   Amazon EventBridge         │  (S3 → EventBridge is native;
│   Rule: "NewIngestionObject" │   no S3 Event Notification+SNS
└─────────────┬───────────────┘   plumbing needed)
              │
              ▼
┌───────────────────────────┐
│   Amazon SQS                 │  Buffers events — decouples arrival
│   Queue: rag-ingestion-queue  │  rate from Airflow's polling/trigger
└─────────────┬───────────────┘  rate; also gives us DLQ + redrive
              │
              ▼
┌─────────────────────────────────────────────┐
│  MWAA — Airflow Triggerer (deferrable sensor)  │
│  DAG: rag_ingestion_sensor_dag                  │
│  Uses SqsSensor (deferrable=True) to poll the    │
│  queue asynchronously — near-zero worker cost     │
│  while idle, event-driven wake-up on new message  │
└─────────────┬─────────────────────────────────┘
              │  message received → triggers a DAG run
              │  (TriggerDagRunOperator, conf={s3_key, modality, doc_id})
              ▼
┌─────────────────────────────────────────────┐
│  MWAA DAG: rag_ingestion_dag                    │
│  (one DAG run per object; dynamic task mapping   │
│   used only for batch/backfill invocations)       │
│                                                    │
│  route_modality (BranchPythonOperator)             │
│        │                                            │
│  ┌─────┼─────────┬─────────────┬─────────────┐     │
│  ▼     ▼         ▼             ▼             ▼     │
│ parse_text parse_image parse_audio* parse_video*    │
│                                                      │
│  *audio/video parse tasks are DEFERRABLE operators   │
│   — they call transcribe.start_transcription_job,    │
│   then defer (suspend) until a TranscribeJobTrigger   │
│   (asyncio, running in the Triggerer) detects          │
│   COMPLETED/FAILED via polling the Transcribe API      │
│   — no worker slot held during the wait                │
│                                                      │
│        ▼ (all branches converge)                    │
│      validate → cleanup → pii_redact → chunk          │
│        → tag_metadata → embed_and_index                │
│                                                      │
│  on_failure_callback (every task) → quarantine_and_   │
│  manifest_update (DynamoDB + S3 quarantine copy)        │
└─────────────┬─────────────────────────────────┘
              ▼
┌───────────────────────────┐
│   Amazon Bedrock              │  Titan Text / Titan Multimodal
│   (embed_and_index task)      │  embeddings
└─────────────┬───────────────┘
              ▼
┌───────────────────────────┐
│   S3 Vectors (Target)         │  single shared index,
│                                │  filter by `modality`
└───────────────────────────┘
```

---

## 3. Why SQS Between EventBridge and Airflow (not a direct trigger)

- **Burst absorption**: a batch upload of 500 video files shouldn't fire 500 simultaneous DAG runs against MWAA's scheduler at once — SQS lets the Airflow sensor pull at a controlled rate.
- **Built-in retry/DLQ**: failed message processing goes to a Dead Letter Queue automatically, separate from the pipeline's own quarantine logic (this catches *orchestration*-level failures, quarantine catches *data-quality* failures).
- **Decoupling**: MWAA environment restarts/deploys don't lose in-flight events — they sit safely in SQS until the sensor is back.

---

## 4. DAG Design

### 4.1 `dags/rag_ingestion_sensor_dag.py` — the event listener

```python
from airflow import DAG
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.decorators import task
from datetime import datetime
import json

with DAG(
    dag_id="rag_ingestion_sensor_dag",
    schedule="@continuous",       # always running, event-driven via deferral
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["rag", "event-driven"],
) as dag:

    poll_queue = SqsSensor(
        task_id="poll_ingestion_queue",
        sqs_queue="https://sqs.<region>.amazonaws.com/<account>/rag-ingestion-queue",
        max_messages=10,
        wait_time_seconds=20,      # SQS long polling
        deferrable=True,           # <-- key: frees worker slot while waiting
    )

    @task
    def parse_messages(messages: list) -> list[dict]:
        parsed = []
        for m in messages:
            body = json.loads(m["Body"])
            detail = body.get("detail", {})
            s3_key = detail["object"]["key"]
            bucket = detail["bucket"]["name"]
            parsed.append({"s3_key": s3_key, "bucket": bucket})
        return parsed

    trigger_ingestion = TriggerDagRunOperator.partial(
        task_id="trigger_rag_ingestion_dag",
        trigger_dag_id="rag_ingestion_dag",
        wait_for_completion=False,
    ).expand(conf=parse_messages(poll_queue.output))
```

### 4.2 `dags/rag_ingestion_dag.py` — the actual pipeline (per object)

This is where your existing modules plug in directly — each Airflow task is a thin wrapper calling into `parsers.py` / `validators.py` / `cleanup.py` / `privacy.py` / `chunker.py` / `metadata.py` / `index_manager.py`, unchanged.

```python
from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime

import config
import parsers
import validators
import cleanup
import privacy
import chunker
import index_manager
from quarantine_utils import quarantine_and_manifest_update  # new thin helper, see §6

default_args = {
    "retries": 2,
    "retry_delay": 60,           # seconds; Airflow handles backoff
    "on_failure_callback": quarantine_and_manifest_update,
}

with DAG(
    dag_id="rag_ingestion_dag",
    schedule=None,               # triggered only, never scheduled directly
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["rag", "ingestion"],
) as dag:

    def _route_modality(**context):
        s3_key = context["dag_run"].conf["s3_key"]
        modality = config.modality_for_key(s3_key)
        context["ti"].xcom_push(key="modality", value=modality)
        return f"parse_{modality}"

    route_modality = BranchPythonOperator(
        task_id="route_modality",
        python_callable=_route_modality,
    )

    def _parse(modality, **context):
        s3_key = context["dag_run"].conf["s3_key"]
        doc_id = context["dag_run"].conf.get("doc_id") or context["run_id"]
        parser = parsers.get_parser(modality)
        doc = parser.parse(s3_key, doc_id)
        context["ti"].xcom_push(key="parsed_doc", value=doc.__dict__)
        return doc

    parse_text = PythonOperator(task_id="parse_text", python_callable=_parse, op_kwargs={"modality": "text"})
    parse_image = PythonOperator(task_id="parse_image", python_callable=_parse, op_kwargs={"modality": "image"})

    # Audio/Video parsing is deferrable — see §5 for the custom operator
    from operators.transcribe_deferrable import TranscribeParseOperator
    parse_audio = TranscribeParseOperator(task_id="parse_audio", modality="audio")
    parse_video = TranscribeParseOperator(task_id="parse_video", modality="video")

    def _validate(**context):
        # reconstruct ParsedDocument from XCom, call validators.validate(doc)
        ...

    validate = PythonOperator(
        task_id="validate",
        python_callable=_validate,
        trigger_rule=TriggerRule.ONE_SUCCESS,   # converge from any modality branch
    )

    def _cleanup(**context): ...
    def _pii_redact(**context): ...
    def _chunk(**context): ...
    def _tag_and_index(**context): ...

    cleanup_task = PythonOperator(task_id="cleanup", python_callable=_cleanup)
    pii_redact_task = PythonOperator(task_id="pii_redact", python_callable=_pii_redact)
    chunk_task = PythonOperator(task_id="chunk", python_callable=_chunk)
    embed_and_index_task = PythonOperator(task_id="embed_and_index", python_callable=_tag_and_index)

    route_modality >> [parse_text, parse_image, parse_audio, parse_video]
    [parse_text, parse_image, parse_audio, parse_video] >> validate
    validate >> cleanup_task >> pii_redact_task >> chunk_task >> embed_and_index_task
```

---

## 5. Deferrable Operator for Transcribe (the key event-driven piece)

Instead of a task that blocks a worker slot calling `transcribe.get_transcription_job` in a `while` loop, Airflow's **Triggerer** process runs an `asyncio`-based trigger that polls in the background across *all* deferred tasks cheaply, and only wakes the task up when the job actually completes — this is what makes the design "event-driven" inside Airflow itself, not just at the S3→SQS entry point.

```python
# operators/transcribe_deferrable.py
from airflow.models import BaseOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent
import asyncio
import boto3


class TranscribeJobTrigger(BaseTrigger):
    def __init__(self, job_name: str, region: str):
        super().__init__()
        self.job_name = job_name
        self.region = region

    def serialize(self):
        return (
            "operators.transcribe_deferrable.TranscribeJobTrigger",
            {"job_name": self.job_name, "region": self.region},
        )

    async def run(self):
        client = boto3.client("transcribe", region_name=self.region)
        while True:
            resp = client.get_transcription_job(TranscriptionJobName=self.job_name)
            status = resp["TranscriptionJob"]["TranscriptionJobStatus"]
            if status in ("COMPLETED", "FAILED"):
                yield TriggerEvent({"status": status, "job_name": self.job_name})
                return
            await asyncio.sleep(15)   # non-blocking; runs in the Triggerer's event loop


class TranscribeParseOperator(BaseOperator):
    def __init__(self, modality: str, **kwargs):
        super().__init__(**kwargs)
        self.modality = modality

    def execute(self, context):
        import parsers
        s3_key = context["dag_run"].conf["s3_key"]
        doc_id = context["dag_run"].conf.get("doc_id") or context["run_id"]

        # Start the async Transcribe job (audio extracted first if video)
        job_name = parsers.start_transcription_job(s3_key, self.modality, doc_id)

        self.defer(
            trigger=TranscribeJobTrigger(job_name=job_name, region="us-east-1"),
            method_name="resume_after_transcription",
        )

    def resume_after_transcription(self, context, event=None):
        if event["status"] == "FAILED":
            raise RuntimeError(f"Transcribe job {event['job_name']} failed")

        import parsers
        s3_key = context["dag_run"].conf["s3_key"]
        doc_id = context["dag_run"].conf.get("doc_id") or context["run_id"]
        doc = parsers.finish_parse_from_transcript(s3_key, self.modality, doc_id, event["job_name"])
        context["ti"].xcom_push(key="parsed_doc", value=doc.__dict__)
```

`parsers.py` needs two small additions to support this split (start job / finish parse) — everything else in the module is untouched:

```python
# parsers.py — additive functions, alongside the existing classes
def start_transcription_job(s3_key: str, modality: str, doc_id: str) -> str:
    """Kick off (but don't wait for) a Transcribe job; for video, extract the
    audio track first. Returns the Transcribe job name."""
    ...

def finish_parse_from_transcript(s3_key: str, modality: str, doc_id: str, job_name: str) -> ParsedDocument:
    """Called after the Triggerer confirms COMPLETED — reads the transcript
    JSON from S3 and builds the same ParsedDocument the synchronous
    AudioParser/VideoParser.parse() would have produced."""
    ...
```

---

## 6. Failure Handling (Airflow-native, same manifest pattern)

```python
# quarantine_utils.py — new thin helper
import boto3
import config

def quarantine_and_manifest_update(context):
    """on_failure_callback for every task in rag_ingestion_dag."""
    dynamodb = boto3.resource("dynamodb", region_name=config.AWS_REGION)
    table = dynamodb.Table(config.MANIFEST_TABLE_NAME)

    dag_run = context["dag_run"]
    task_instance = context["task_instance"]
    s3_key = dag_run.conf["s3_key"]
    doc_id = dag_run.conf.get("doc_id") or dag_run.run_id

    table.put_item(Item={
        "doc_id": doc_id,
        "s3_key": s3_key,
        "status": "failed",
        "failed_task": task_instance.task_id,
        "reason": str(context.get("exception")),
        "airflow_run_id": dag_run.run_id,
        "timestamp": context["ts"],
    })

    modality = config.modality_for_key(s3_key)
    s3 = boto3.client("s3", region_name=config.AWS_REGION)
    s3.copy_object(
        Bucket=config.QUARANTINE_BUCKET,
        CopySource={"Bucket": config.SOURCE_BUCKET, "Key": s3_key},
        Key=f"{config.QUARANTINE_PREFIX[modality]}{s3_key.split('/')[-1]}",
    )
```

Airflow's built-in `retries`/`retry_delay` in `default_args` handles transient failures (e.g. throttling on Bedrock/Textract) before this callback ever fires — the quarantine path only triggers after retries are exhausted.

---

## 7. Repo Structure — Final Layout with Airflow Added

```
repo/
├── dags/
│   ├── rag_ingestion_sensor_dag.py     (NEW — event listener, SQS deferrable sensor)
│   └── rag_ingestion_dag.py            (NEW — per-object pipeline, wraps existing modules)
├── operators/
│   └── transcribe_deferrable.py        (NEW — custom deferrable operator)
├── config.py                            (extended: MANIFEST_TABLE_NAME, SQS queue URL, Airflow conn IDs)
├── parsers.py                           (extended: start_transcription_job / finish_parse_from_transcript)
├── validators.py                        (unchanged logic, called from Airflow task)
├── cleanup.py                           (unchanged logic, called from Airflow task)
├── privacy.py                           (unchanged logic, called from Airflow task)
├── chunker.py                           (unchanged logic, called from Airflow task)
├── metadata.py                          (unchanged logic, called from Airflow task)
├── index_manager.py                     (unchanged logic, called from Airflow task)
├── embeddings.py                        (unchanged — Bedrock wrapper)
├── media_utils.py                       (unchanged — ffmpeg/ffprobe/perceptual hash helpers)
├── quarantine_utils.py                  (NEW — on_failure_callback helper)
├── delete_vectors_manifest.py           (unchanged; can itself become a manual-trigger DAG later)
├── evaluate.py                          (unchanged; can run as a scheduled Airflow DAG separately)
├── requirements.txt                     (add: apache-airflow-providers-amazon)
└── README.md
```

**`pipeline.py` is retired** in this architecture — its orchestration responsibility is now split across `dags/rag_ingestion_sensor_dag.py` (event listening) and `dags/rag_ingestion_dag.py` (stage sequencing). If you want to keep `pipeline.py` as a callable CLI for local/manual single-file testing outside Airflow, it can remain as a thin wrapper that calls the same task functions directly, without Airflow — useful for `test_credentials.py`-style smoke tests.

---

## 8. MWAA Environment Considerations

- **Requires a Triggerer**: MWAA supports the Triggerer component (Airflow 2.4+) needed for deferrable operators — confirm your MWAA environment version supports it before committing to this design.
- **Environment class**: deferred tasks are cheap (event-loop based), so worker count can stay modest even under high video-ingestion volume; size workers for the synchronous stages (ffmpeg frame extraction, Bedrock embedding calls) instead.
- **VPC networking**: MWAA runs in your VPC — Transcribe/Textract/Rekognition/Bedrock/S3 Vectors calls need VPC endpoints (PrivateLink) or NAT gateway egress, same as any Lambda-based design would.
- **DAG-level concurrency**: set `max_active_runs` per DAG and pool-based concurrency limits on Bedrock-calling tasks specifically, since Bedrock has account-level TPS quotas that a burst of parallel video-embedding tasks could hit.

---

## 9. Open Decisions

1. **Confirm MWAA Triggerer availability** in your environment (Airflow version) — this entire deferrable-operator design depends on it. If unavailable, fallback is a `PokeReturnValue`/reschedule-mode sensor (worker-slot-light but not zero-cost) instead of true deferral.
2. **SQS vs direct EventBridge→MWAA trigger**: SQS adds a buffering/DLQ layer; if you'd rather trigger DAG runs directly from EventBridge (via an EventBridge target calling the MWAA REST API), that removes SQS but loses the burst-buffering behavior — which do you want?
3. **`pipeline.py` retirement**: confirm you're fine deprecating it as the orchestrator in favor of the two DAGs, keeping it only as an optional local-test CLI.
4. **Bedrock concurrency limits**: should the framework enforce an Airflow **pool** (e.g. `bedrock_embedding_pool`, max 5 concurrent slots) to protect against quota throttling during large video backfills?

Once confirmed, I can generate the complete working DAG files, the custom operator, and the updated `parsers.py`/`config.py` diffs as ready-to-drop-in code.
