# monitoring_agent/monitor.py

import os
import logging
import requests
import datetime
from datetime import timedelta, timezone
import time
import dateutil.tz

from monitoring_agent.context_store import ContextStoreSQLite
from monitoring_agent.azure_ad_integration import AzureAuthClient
from shared.schemas import FailureContext
from shared.config import POLL_INTERVAL_SECONDS, RETRY_THRESHOLD
from decision_agent.decision_logic import DecisionLogicAgent
from trigger_agent.trigger_runner import TriggerAgent


logger = logging.getLogger(__name__)

class MonitoringAgent:
    def __init__(self):
        self.subscription_id = os.getenv("AZURE_SUBSCRIPTION_ID")
        self.resource_group = os.getenv("RESOURCE_GROUP_NAME")
        self.data_factory = os.getenv("DATA_FACTORY_NAME")
        self.api_version = "2018-06-01"
        self.poll_interval = POLL_INTERVAL_SECONDS
        self.context_store = ContextStoreSQLite()
        self.auth_client = AzureAuthClient()
        self.decision_agent = DecisionLogicAgent()
        self.trigger_agent = TriggerAgent()

        self.adf_base_url = (
            f"https://management.azure.com/subscriptions/{self.subscription_id}"
            f"/resourceGroups/{self.resource_group}/providers/Microsoft.DataFactory"
            f"/factories/{self.data_factory}"
        )

    def get_access_token(self):
        try:
            return self.auth_client.get_token()
        except Exception as e:
            logger.error(f"Failed to fetch Azure AD token: {e}")
            raise

    def query_pipeline_runs(self, access_token, since_time):
        url = f"{self.adf_base_url}/queryPipelineRuns?api-version={self.api_version}"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        params = {
            "lastUpdatedAfter": since_time.isoformat(),
            "lastUpdatedBefore": datetime.datetime.now(timezone.utc).isoformat(),
            "filters": [
                {"operand": "Status", "operator": "In", "values": ["Failed", "Succeeded"]}
            ]
        }
        response = requests.post(url, json=params, headers=headers)
        if not response.ok:
            logger.error(f"Failed to query pipeline runs: {response.text}")
            raise Exception(f"ADF API error: {response.text}")
        return response.json().get("value", [])

    def get_failed_activity(self, access_token, run_id):
        url = f"{self.adf_base_url}/pipelineruns/{run_id}/queryActivityRuns?api-version={self.api_version}"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        body = {
            "lastUpdatedAfter": (datetime.datetime.now(timezone.utc) - timedelta(days=1)).isoformat(),
            "lastUpdatedBefore": datetime.datetime.now(timezone.utc).isoformat()
        }
        response = requests.post(url, json=body, headers=headers)
        if not response.ok:
            logger.error(f"Failed to query activity runs for run {run_id}: {response.text}")
            return None
        activities = response.json().get("value", [])
        for act in activities:
            if act.get("status") == "Failed":
                return act.get("activityName")
        return None

    def process_failures(self, failures):
        """
        For each failure, confirm with user → ask GPT + RAG → decide rerun or escalate.
        """
        for failure in failures:
            run_id = failure.run_id
            pipeline_name = failure.pipeline_name

            retries_left = self.context_store.get_retry_count(run_id) or RETRY_THRESHOLD
            if retries_left < 1:
                logger.info(f"Run {run_id} has no retries left. Escalating to manual intervention.")
                # Escalation notification here
                continue

            # Ask user confirmation
            confirm = input(f"Run {run_id} of {pipeline_name} failed. Retry? (y/n): ").strip().lower()
            if confirm != "y":
                logger.info(f"User declined retry for run {run_id}. Escalating...")
                self.context_store.set_retry_count(run_id, 0)   # ✅ fixed method name
                # Escalation notification here
                continue

            # AI decision (with RAG context)
            decision = self.decision_agent.make_decision(failure)
            logger.info(f"AI Decision for {pipeline_name} ({run_id}): {decision}")

            if decision["action"] == "no_rerun":
                logger.info(f"No rerun suggested for run {run_id}. Escalating.")
                self.context_store.set_retry_count(run_id, 0)
                continue

            # ✅ Execute decision using TriggerAgent (real rerun in ADF)
            try:
                self.trigger_agent.execute_decision(decision, failure)
                logger.info(f"Triggered retry for {pipeline_name} (orig={run_id}) via TriggerAgent")
            except Exception as e:
                logger.error(f"Failed to trigger retry for run {run_id}: {e}")
                self.context_store.set_retry_count(run_id, 0)
                continue

            # ✅ Decrement retry count since we attempted a rerun
            self.context_store.set_retry_count(run_id, retries_left - 1)


    def poll(self):
        logger.info("Starting monitoring loop.")
        while True:
            now_utc = datetime.datetime.now(timezone.utc)
            lookback_duration = timedelta(hours=10)  # Always check last 3 hours
            since_time = now_utc - lookback_duration

            local_tz = dateutil.tz.tzlocal()
            logger.info(
                f"Polling ADF pipeline runs (Failed + Succeeded) since "
                f"UTC {since_time.isoformat()} / local {since_time.astimezone(local_tz).isoformat()}"
            )

            try:
                token = self.get_access_token()
                runs = self.query_pipeline_runs(token, since_time)
            except Exception as e:
                logger.error(f"Polling failed: {e}")
                time.sleep(self.poll_interval)
                continue

            failures = []
            success_count = 0
            failure_count = 0

            for run in runs:
                run_id = run.get("runId")
                pipeline_name = run.get("pipelineName")
                status = run.get("status", "Unknown")
                error_message = run.get("message", "No error message available")

                if not run_id:
                    continue

                if status == "Succeeded":
                    success_count += 1
                    logger.info(f"Pipeline run succeeded: run_id={run_id}, pipeline={pipeline_name}")
                    self.context_store.create_or_update_run(run_id, pipeline_name, "Succeeded", RETRY_THRESHOLD)
                    continue

                if status == "Failed":
                    failure_count += 1
                    failed_activity = self.get_failed_activity(token, run_id)

                    failure_context = FailureContext(
                        pipeline_name=pipeline_name,
                        run_id=run_id,
                        status=status,
                        error_message=error_message,
                        failed_activity=failed_activity,
                        timestamp=now_utc
                    )
                    failures.append(failure_context)

            logger.info(
                f"Discovered {failure_count} failed and {success_count} succeeded pipeline runs. "
                f"Current UTC: {now_utc.isoformat()} / local: {now_utc.astimezone(local_tz).isoformat()}"
            )

            # Process failures with retry loop + user confirmation + RAG
            if failures:
                self.process_failures(failures)

            # Sleep until next poll
            logger.info(f"Sleeping {self.poll_interval} seconds before next poll...")
            time.sleep(self.poll_interval)
