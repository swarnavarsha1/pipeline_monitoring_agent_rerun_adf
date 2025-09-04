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
from notification_agent.notifier import Notifier 
from knowledge_base.solution_retriever import RAGSolutionRetriever
from shared.utils import setup_logger

logger = setup_logger("Monitoring_Agent")

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
        self.notifier = Notifier() 

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
        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
        params = {
            "lastUpdatedAfter": since_time.isoformat(),
            "lastUpdatedBefore": datetime.datetime.now(timezone.utc).isoformat(),
            "filters": [{"operand": "Status", "operator": "In", "values": ["Failed", "Succeeded"]}],
        }
        response = requests.post(url, json=params, headers=headers)
        if not response.ok:
            logger.error(f"Failed to query pipeline runs: {response.text}")
            raise Exception(f"ADF API error: {response.text}")
        return response.json().get("value", [])

    def get_failed_activity(self, access_token, run_id):
        url = f"{self.adf_base_url}/pipelineruns/{run_id}/queryActivityRuns?api-version={self.api_version}"
        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
        body = {
            "lastUpdatedAfter": (datetime.datetime.now(timezone.utc) - timedelta(days=1)).isoformat(),
            "lastUpdatedBefore": datetime.datetime.now(timezone.utc).isoformat(),
        }
        response = requests.post(url, json=body, headers=headers)
        if not response.ok:
            logger.error(f"Failed to query activity runs for run {run_id}: {response.text}")
            return None
        for act in response.json().get("value", []):
            if act.get("status") == "Failed":
                return act.get("activityName")
        return None

    def process_failures(self, failures):
        for failure in failures:
            run_id = failure.run_id
            pipeline_name = failure.pipeline_name

            retries_left = self.context_store.get_retry_count(run_id) or RETRY_THRESHOLD
            if retries_left < 1:
                logger.info(f"Run {run_id} has no retries left. Escalating.")
                self.context_store.set_retry_count(run_id, 0)
                continue

            confirm = input(f"Run {run_id} of {pipeline_name} failed. Retry? (y/n): ").strip().lower()
            if confirm != "y":
                logger.info(f"User declined retry for run {run_id}. Escalating...")

                self.context_store.set_retry_count(run_id, 0)

                # 🔔 Notify even when user declines
                self.notifier.notify_failure(
                    failure,
                    {"action": "no_rerun", "reason": "User declined retry"},
                    rerun_outcome=None,
                    solution=None
                )

                continue

            # ✅ AI Decision
            ai_result = self.decision_agent.make_decision(failure)

            rag = RAGSolutionRetriever()
            rag_solution = rag.get_solution(ai_result["reason"])

            logger.info("\n     AI Response for failure:")
            logger.info(f"      Action   : {ai_result['action']}")
            logger.info(f"      Reason   : {ai_result['reason']}\n")
            logger.info(f"      Suggested Solution:\n      {rag_solution}\n")

            rerun_outcome = None
            if ai_result["action"] != "no_rerun":
                try:
                    rerun_outcome = self.trigger_agent.execute_decision(ai_result, failure)
                    logger.info(f"Triggered retry for {pipeline_name} (orig={run_id}) via TriggerAgent")
                except Exception as e:
                    rerun_outcome = {"error": str(e)}
                    self.context_store.set_retry_count(run_id, 0)

            # ✅ Send consolidated notification (ONLY once per failure)
            self.notifier.notify_failure(
                failure,
                ai_result,
                rerun_outcome,
                solution=rag_solution
            )

            # ✅ Update retry counter
            self.context_store.set_retry_count(run_id, retries_left - 1)



    def poll(self):
        logger.info("Starting monitoring loop.")
        while True:
            now_utc = datetime.datetime.now(timezone.utc)
            since_time = now_utc - timedelta(hours=24)
            logger.info(f"Polling ADF pipeline runs since {since_time}")

            try:
                token = self.get_access_token()
                runs = self.query_pipeline_runs(token, since_time)
            except Exception as e:
                logger.error(f"Polling failed: {e}")
                time.sleep(self.poll_interval)
                continue

            failures, success_count, failure_count = [], 0, 0

            for run in runs:
                run_id, pipeline_name = run.get("runId"), run.get("pipelineName")
                status = run.get("status", "Unknown")
                error_message = run.get("message", "No error message available")
                if not run_id:
                    continue

                if status == "Succeeded":
                    success_count += 1
                    logger.info(f"Pipeline run succeeded: run_id={run_id}, pipeline={pipeline_name}")
                    self.context_store.create_or_update_run(run_id, pipeline_name, "Succeeded", RETRY_THRESHOLD)
                elif status == "Failed":
                    failure_count += 1
                    db_status = self.context_store.get_status(run_id)
                    if db_status in ["retrying", "succeeded", "failed_no_retry", "superseded"]:
                        logger.info(f"Skipping run {run_id} (DB status={db_status})")
                        continue
                    if (self.context_store.get_retry_count(run_id) or RETRY_THRESHOLD) < 1:
                        logger.info(f"Run {run_id} has no retries left. Skipping.")
                        continue
                    failed_activity = self.get_failed_activity(token, run_id)
                    failures.append(
                        FailureContext(
                            pipeline_name=pipeline_name,
                            run_id=run_id,
                            status=status,
                            error_message=error_message,
                            failed_activity=failed_activity,
                            timestamp=now_utc,
                        )
                    )

            logger.info(f"Discovered {failure_count} failed and {success_count} succeeded pipeline runs.")
            if failures:
                self.process_failures(failures)

            logger.info(f"Sleeping {self.poll_interval} seconds before next poll...")
            time.sleep(self.poll_interval)
