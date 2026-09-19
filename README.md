# dag-failure-agent

## One-time GCP setup

```
gcloud services enable run.googleapis.com pubsub.googleapis.com \
  firestore.googleapis.com secretmanager.googleapis.com \
  logging.googleapis.com aiplatform.googleapis.com storage.googleapis.com

gcloud firestore databases create --location=us-central1

gsutil mb -l us-central1 gs://YOUR-REPOS-CONFIG-BUCKET

gcloud pubsub topics create dagfailures
gcloud pubsub subscriptions create dagfailures-push \
  --topic=dagfailures \
  --push-endpoint=https://YOUR-CLOUD-RUN-URL/failure \
  --push-auth-service-account=YOUR-SERVICE-ACCOUNT
```

Create two Secret Manager secrets before the first run:

```
gcloud secrets create github-webhook-secret --data-file=- <<< "your-webhook-secret"
gcloud secrets create github-token-yourrepo --data-file=- <<< "ghp_your_pat"
```

## Deploy

```
gcloud builds submit --config cloudbuild.yaml
```

Set env vars on the Cloud Run service (see `.env.example`) plus grant the
service's runtime identity: Firestore user, Secret Manager secret accessor
(on the specific secrets), Storage object admin (on the repos bucket),
Logging viewer, and Pub/Sub publisher (for the Composer environment's
service account, to publish to `dagfailures`).

## Register a repo

Open the Cloud Run URL (`/`), fill in the "Add repo" form: `owner/repo`,
the Secret Manager secret name holding its PAT, and a path template like
`dags/{dag_id}.py`. Check "default" for your primary repo.

## Wire up Composer

Copy `dags/agent_failure_callback.py` into your Composer environment's
`dags/` (or a `plugins/`-adjacent import path), then in an
`airflow_local_settings.py` policy (or directly in each DAG's
`default_args`):

```python
from agent_failure_callback import notify_dag_failure_agent

default_args = {"on_failure_callback": notify_dag_failure_agent}
```

Set `GCP_PROJECT` (and `AGENT_FAILURE_TOPIC` if you didn't use the default
name `dagfailures`) in the Composer environment's Airflow env vars.

## GitHub webhook

On each registered repo, add a webhook: URL
`https://YOUR-CLOUD-RUN-URL/github-webhook`, content type
`application/json`, secret matching `github-webhook-secret`, events:
just "Pull requests".

## First real test

Trigger one real failing task in Composer. Watch Cloud Run logs, then
check `/` for the run to appear, gate or open a PR, and — once you close
or merge that PR — watch the Weights table change.
