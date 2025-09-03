# feedback_agent/feedback_loop.py

import logging
from monitoring_agent.context_store import ContextStoreSQLite
from shared.utils import send_email
from shared.config import RETRY_THRESHOLD, ALERT_RECIPIENTS

logger = logging.getLogger(__name__)

class FeedbackAgent:
    def __init__(self):
        self.context_store = ContextStoreSQLite()
        self.alert_recipients = ALERT_RECIPIENTS

    def process_decision_outcome(self, decision, failure_context):
        run_id = failure_context.run_id
        pipeline_id = failure_context.pipeline_name
        action = decision.get("action")
        reason = decision.get("reason", "No reason specified")
        status = failure_context.status

        retry_count = self.context_store.get_retry_count(run_id)

        if action in ["full_rerun", "partial_rerun"]:
            if retry_count == RETRY_THRESHOLD:
                # First retry → ask for confirmation
                user_wants_retry = self.ask_user_confirmation(run_id, pipeline_id, reason, failure_context)
                if not user_wants_retry:
                    self.context_store.set_retry_count(run_id, 0)
                    self.context_store.update_status(run_id, "failed_no_retry")
                    self.send_alert(
                        failure_context,
                        retry_count,
                        "User denied retry confirmation. Manual intervention required."
                    )
                    logger.info(f"User denied retry for run {run_id}, stopped further retries.")
                    return
                retry_count = RETRY_THRESHOLD - 1
                self.context_store.set_retry_count(run_id, retry_count)
                self.context_store.update_status(run_id, "retrying")
                logger.info(f"User confirmed first retry for run {run_id}, set retry_count to {retry_count}")
            elif retry_count > 0:
                retry_count = retry_count - 1
                self.context_store.set_retry_count(run_id, retry_count)
                self.context_store.update_status(run_id, "retrying")
                logger.info(f"Retrying run {run_id}, retry_count now {retry_count}")
            else:  # retry_count == 0
                self.context_store.update_status(run_id, "failed_no_retry")
                self.send_alert(failure_context, retry_count, "Final retry failed; retries exhausted.")
                logger.info(f"Retries exhausted for run {run_id}, no further retries.")
                return

        elif action == "no_rerun":
            self.context_store.set_retry_count(run_id, 0)
            self.context_store.update_status(run_id, "failed_no_retry")
            self.send_alert(failure_context, retry_count, f"No rerun (reason: {reason})")
            logger.info(f"No rerun decision for run {run_id}: {reason}")

        elif action == "success":
            self.context_store.set_retry_count(run_id, RETRY_THRESHOLD)
            self.context_store.update_status(run_id, "succeeded")
            logger.info(f"Run {run_id} succeeded, retry count reset to {RETRY_THRESHOLD}")

        else:
            logger.info(f"Decision action '{action}' not recognized for run {run_id}.")

    def ask_user_confirmation(self, run_id, pipeline_id, reason, failure_context):
        subject = f"[ACTION REQUIRED] Retry confirmation for pipeline '{pipeline_id}' run {run_id}"
        body = (
            f"Pipeline '{pipeline_id}' run {run_id} failed.\n"
            f"Failure reason:\n{reason}\n\n"
            "Please confirm if you want to retry the pipeline.\n"
            "(Replace this simulation with a real interactive mechanism.)"
        )
        if self.alert_recipients:
            send_email(subject, body, self.alert_recipients)
            logger.info(f"Sent user confirmation request email for retry on run {run_id}")

        # ✅ Simulated approval - replace with actual mechanism later
        return True

    def send_alert(self, failure_context, retry_count, reason):
        subject = f"[ALERT] Pipeline '{failure_context.pipeline_name}' run {failure_context.run_id} requires manual intervention"
        body = (
            f"Pipeline Name: {failure_context.pipeline_name}\n"
            f"Run ID: {failure_context.run_id}\n"
            f"Failed Activity: {failure_context.failed_activity}\n"
            f"Error Message: {failure_context.error_message}\n"
            f"Retry Attempts Left: {retry_count}\n"
            f"Reason: {reason}\n"
        )
        if self.alert_recipients:
            send_email(subject, body, self.alert_recipients)
            logger.info(f"Sent alert email for run {failure_context.run_id}")
        else:
            logger.warning("No alert recipients configured; alert email not sent.")
