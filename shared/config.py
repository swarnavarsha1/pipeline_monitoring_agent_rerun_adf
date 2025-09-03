import os

# Core Azure AD and Azure Data Factory config
AZURE_TENANT_ID = os.getenv("AZURE_TENANT_ID")
AZURE_CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
AZURE_CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")

AZURE_SUBSCRIPTION_ID = os.getenv("AZURE_SUBSCRIPTION_ID")
RESOURCE_GROUP_NAME = os.getenv("RESOURCE_GROUP_NAME")
DATA_FACTORY_NAME = os.getenv("DATA_FACTORY_NAME")

# OpenAI API key
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Email and notification settings
EMAIL_SMTP_SERVER = os.getenv("EMAIL_SMTP_SERVER", "smtp.gmail.com")
EMAIL_SMTP_PORT = int(os.getenv("EMAIL_SMTP_PORT", "587"))
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
ALERT_RECIPIENTS = [
    email.strip()
    for email in os.getenv("ALERT_RECIPIENTS", "").split(",")
    if email.strip()
]

# Polling and retry settings
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", 300))
RETRY_THRESHOLD = int(os.getenv("RETRY_THRESHOLD", 2))
