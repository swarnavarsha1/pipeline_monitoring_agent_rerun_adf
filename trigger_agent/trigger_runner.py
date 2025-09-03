# trigger_agent/trigger_runner.py

import os
import logging
import requests
from monitoring_agent.context_store import ContextStoreSQLite
from monitoring_agent.azure_ad_integration import AzureAuthClient

logger = logging.getLogger(__name__)

class TriggerAgent:
    def __init__(self):
        self.subscription_id = os.getenv("AZURE_SUBSCRIPTION_ID")
        self.resource_group = os.getenv("RESOURCE_GROUP_NAME")
        self.data_factory = os.getenv("DATA_FACTORY_NAME")
        self.api_version = "2018-06-01"
        self.auth_client = AzureAuthClient()
        self.context_store = ContextStoreSQLite()
        self.adf_base_url = (
            f"https://management.azure.com/subscriptions/{self.subscription_id}"
            f"/resourceGroups/{self.resource_group}/providers/Microsoft.DataFactory"
            f"/factories/{self.data_factory}"
        )

    def execute_decision(self, decision: dict, failure_context):
        run_id = failure_context.run_id
        pipeline_name = failure_context.pipeline_name

        action = decision.get("action")
        reason = decision.get("reason", "")

        logger.info(
            f"TriggerAgent: run_id={run_id} pipeline={pipeline_name} action={action}"
        )

        if action == "no_rerun":
            logger.info(f"Decision: no rerun for run {run_id}, reason: {reason}")
            self.context_store.update_status(run_id, "failed_no_retry")
            return

        is_recovery = action == "partial_rerun"
        url = f"{self.adf_base_url}/pipelines/{pipeline_name}/createRun?api-version={self.api_version}"

        params = {
            "referencePipelineRunId": run_id,
            "isRecovery": str(is_recovery).lower()
        }
        if is_recovery:
            if getattr(failure_context, "failed_activity", None):
                params["startActivityName"] = failure_context.failed_activity
            else:
                params["startFromFailure"] = "true"

        try:
            access_token = self.auth_client.get_token()
        except Exception as e:
            logger.error(f"Failed to acquire Azure AD token: {e}")
            return

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

        try:
            response = requests.post(url, headers=headers, params=params)
            if response.status_code not in [200, 201, 202]:
                logger.error(f"Failed to trigger pipeline rerun: {response.status_code} - {response.text}")
                self.context_store.update_status(run_id, "failed_rerun_error")
                return
            run_info = response.json()
            new_run_id = run_info.get("runId", "unknown")
            logger.info(f"Pipeline rerun triggered: new_run_id={new_run_id} (reason: {reason})")
            self.context_store.update_status(run_id, "retrying")

        except Exception as e:
            logger.error(f"Exception during rerun API call for run {run_id}: {e}")
            self.context_store.update_status(run_id, "failed_rerun_exception")
