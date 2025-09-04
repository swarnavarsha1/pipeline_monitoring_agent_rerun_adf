# main.py

import time
import logging
import os
from dotenv import load_dotenv

load_dotenv() 

from shared.utils import setup_logger
from monitoring_agent.monitor import MonitoringAgent
from decision_agent.decision_logic import DecisionLogicAgent
from trigger_agent.trigger_runner import TriggerAgent
from feedback_agent.feedback_loop import FeedbackAgent

def main():
    logger = setup_logger("Main")
    logger.info("Starting Azure Data Factory monitoring pipeline assistant.")

    monitor = MonitoringAgent()
    decision_agent = DecisionLogicAgent()
    trigger_agent = TriggerAgent()
    feedback_agent = FeedbackAgent()

    poll_interval = int(os.getenv("POLL_INTERVAL_SECONDS", 300))

    while True:
        try:
            # Step 1: Detect failed pipelines
            failures = monitor.poll()

            # Step 2-4: Process each failure end-to-end
            for failure_context in failures:
                decision = decision_agent.make_decision(failure_context)
                trigger_agent.execute_decision(decision, failure_context)
                feedback_agent.process_decision_outcome(decision, failure_context)

        except Exception as e:
            logger.error(f"Exception in main monitoring loop: {e}")

        logger.info(f"Waiting {poll_interval} seconds before next poll...")
        time.sleep(poll_interval)

if __name__ == "__main__":
    main()
