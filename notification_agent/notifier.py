# notifier_agent/notifier.py
import smtplib
from email.message import EmailMessage
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

class Notifier:
    def __init__(self):
        self.smtp_server = os.getenv("EMAIL_SMTP_SERVER", "smtp.gmail.com")
        self.smtp_port = int(os.getenv("EMAIL_SMTP_PORT", "587"))
        self.smtp_user = os.getenv("EMAIL_USER")
        self.smtp_password = os.getenv("EMAIL_PASSWORD")
        self.recipient = os.getenv("NOTIFICATION_EMAIL", self.smtp_user)

    def send_email(self, subject: str, body: str):
        if not self.smtp_user or not self.smtp_password:
            print("[Notifier] Missing EMAIL_USER or EMAIL_PASSWORD.")
            return

        msg = EmailMessage()
        msg.set_content(body)
        msg["Subject"] = subject
        msg["From"] = self.smtp_user
        msg["To"] = self.recipient

        try:
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.ehlo()
                server.starttls()
                server.ehlo()
                server.login(self.smtp_user, self.smtp_password)
                server.send_message(msg)
            print(f"[Notifier] Email sent to {self.recipient}")
        except Exception as e:
            print(f"[Notifier] Failed to send email: {e}")

    def notify_failure(self, failure, ai_result, rerun_outcome=None, solution=None):
        pipeline_name = failure.pipeline_name
        run_id = failure.run_id
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

        subject = f"ADF Pipeline Failure — {pipeline_name}"

        body = f"""
Time: {timestamp}
Pipeline: {pipeline_name}
Run ID: {run_id}

AI Decision: {ai_result.get("action", "N/A")}
Reason: {ai_result.get("reason", "N/A")}

Error Message: {failure.error_message or "No error message available"}
Failed Activity: {failure.failed_activity or "Unknown"}

Suggested Solution (from Knowledge Base):
{solution or "No documented solution found."}

Rerun Outcome:
{rerun_outcome or "No rerun attempted."}
"""
        self.send_email(subject, body)

