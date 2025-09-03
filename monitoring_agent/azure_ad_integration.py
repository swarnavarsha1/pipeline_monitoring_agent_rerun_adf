# monitoring_agent/azure_ad_integration.py

import os
import requests
import logging
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

class AzureAuthClient:
    """
    Client to acquire Azure AD OAuth2 token using client credentials flow.
    """

    def __init__(self):
        self.tenant_id = os.getenv("AZURE_TENANT_ID")
        self.client_id = os.getenv("AZURE_CLIENT_ID")
        self.client_secret = os.getenv("AZURE_CLIENT_SECRET")

        missing = []
        if not self.tenant_id:
            missing.append("AZURE_TENANT_ID")
        if not self.client_id:
            missing.append("AZURE_CLIENT_ID")
        if not self.client_secret:
            missing.append("AZURE_CLIENT_SECRET")

        if missing:
            raise EnvironmentError(f"Missing environment variables for Azure AD auth: {', '.join(missing)}")

        self.token_endpoint = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        self.scope = "https://management.azure.com/.default"

    def get_token(self):
        logger.debug(f"Requesting token from {self.token_endpoint}")
        payload = {
            'client_id': self.client_id,
            'scope': self.scope,
            'client_secret': self.client_secret,
            'grant_type': 'client_credentials'
        }
        try:
            response = requests.post(self.token_endpoint, data=payload)
            if response.status_code != 200:
                logger.error(f"Failed to fetch Azure AD token: {response.text}")
                raise Exception(f"Azure AD token request failed: {response.text}")
            token = response.json().get("access_token")
            if not token:
                logger.error("Azure AD token not found in response")
                raise Exception("Missing access token in Azure AD response.")
            logger.debug("Successfully acquired Azure AD token")
            return token
        except requests.RequestException as e:
            logger.error(f"HTTP request exception during token fetch: {e}")
            raise
