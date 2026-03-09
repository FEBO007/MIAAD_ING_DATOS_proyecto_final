import os
import time
import httpx
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger
from prefect_dbt import PrefectDbtRunner

try:
    from prefect.client.schemas.schedules import CronSchedule
except ImportError:
    from prefect.server.schemas.schedules import CronSchedule

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

AIRBYTE_PUBLIC_API_URL = os.getenv("AIRBYTE_PUBLIC_API_URL", "http://localhost:8000/api/public/v1").rstrip("/")
CLIENT_ID = os.getenv("AIRBYTE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AIRBYTE_CLIENT_SECRET")

CONN_NYT = os.getenv("AIRBYTE_CONN_ID_NYT_PARENT_GBOOKS")
CONN_WIKI = os.getenv("AIRBYTE_CONN_ID_WIKI_PAGEVIEWS")

DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR")
DBT_TARGET = os.getenv("DBT_TARGET", "final")
DBT_SELECT = os.getenv("DBT_SELECT", "path:models/proyecto_final")

def _client_creds_ok():
    if not CLIENT_ID or not CLIENT_SECRET:
        raise ValueError("Faltan AIRBYTE_CLIENT_ID / AIRBYTE_CLIENT_SECRET en prefect_flows/.env")

@task(name="Airbyte - Get access token")
def get_access_token() -> str:
    _client_creds_ok()
    with httpx.Client(timeout=60.0) as client:
        r = client.post(
            f"{AIRBYTE_PUBLIC_API_URL}/applications/token",
            headers={"accept": "application/json", "content-type": "application/json"},
            json={
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "grant-type": "client_credentials",
            },
        )
        r.raise_for_status()
        return r.json()["access_token"]

def _auth_header(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "accept": "application/json"}

@task(retries=2, retry_delay_seconds=20, name="Airbyte - Trigger sync")
def trigger_sync(connection_id: str, token: str) -> int:
    logger = get_run_logger()
    if not connection_id:
        raise ValueError("connection_id vacío")

    with httpx.Client(timeout=60.0) as client:
        r = client.post(
            f"{AIRBYTE_PUBLIC_API_URL}/jobs",
            headers={**_auth_header(token), "content-type": "application/json"},
            json={"connectionId": connection_id, "jobType": "sync"},
        )
        r.raise_for_status()
        payload = r.json()

    job_id = payload.get("jobId") or payload.get("id")
    if job_id is None:
        raise RuntimeError(f"No pude leer job id en respuesta: {payload}")

    logger.info(f"Sync disparado. connection_id={connection_id} job_id={job_id}")
    return int(job_id)

@task(name="Airbyte - Wait job")
def wait_job(job_id: int, poll_seconds: int = 15, timeout_minutes: int = 120) -> None:
    logger = get_run_logger()
    deadline = time.time() + timeout_minutes * 60
    token = get_access_token()

    with httpx.Client(timeout=60.0) as client:
        while True:
            if time.time() > deadline:
                raise TimeoutError(f"Timeout esperando job_id={job_id} (> {timeout_minutes} min)")

            r = client.get(f"{AIRBYTE_PUBLIC_API_URL}/jobs/{job_id}", headers=_auth_header(token))
            if r.status_code == 401:
                token = get_access_token()
                r = client.get(f"{AIRBYTE_PUBLIC_API_URL}/jobs/{job_id}", headers=_auth_header(token))

            r.raise_for_status()
            job = r.json()
            status = job.get("status") or job.get("job", {}).get("status")
            logger.info(f"job_id={job_id} status={status}")

            if status in ("succeeded", "success"):
                logger.info("✅ Airbyte sync succeeded")
                return
            if status in ("failed", "cancelled", "canceled", "error"):
                raise RuntimeError(f"❌ Airbyte job terminó en {status}")

            time.sleep(poll_seconds)

@task(name="dbt - deps + build (target final)")
def dbt_deps_and_build() -> None:
    if not DBT_PROJECT_DIR or not DBT_PROFILES_DIR:
        raise ValueError("Faltan DBT_PROJECT_DIR / DBT_PROFILES_DIR en prefect_flows/.env")

    runner = PrefectDbtRunner()

    runner.invoke([
        "deps",
        "--project-dir", DBT_PROJECT_DIR,
        "--profiles-dir", DBT_PROFILES_DIR,
    ])

    runner.invoke([
        "build",
        "--project-dir", DBT_PROJECT_DIR,
        "--profiles-dir", DBT_PROFILES_DIR,
        "--target", DBT_TARGET,
        "--select", DBT_SELECT,
    ])

@flow(name="Proyecto Final - Pipeline (Airbyte + dbt build)")
def airbyte_and_dbt_flow():
    if not CONN_NYT or not CONN_WIKI:
        raise ValueError("Faltan AIRBYTE_CONN_ID_NYT_PARENT_GBOOKS o AIRBYTE_CONN_ID_WIKI_PAGEVIEWS en .env")

    token = get_access_token()

    job_nyt = trigger_sync(CONN_NYT, token)
    wait_job(job_nyt)

    job_wiki = trigger_sync(CONN_WIKI, token)
    wait_job(job_wiki)

    dbt_deps_and_build()

#if __name__ == "__main__":
#    airbyte_and_dbt_flow()

if __name__ == "__main__":
    airbyte_and_dbt_flow.serve(
        name="pf-semanal",
        schedule=CronSchedule(cron="0 6 * * MON", timezone="America/Asuncion"),
        tags=["proyecto-final"],
    )