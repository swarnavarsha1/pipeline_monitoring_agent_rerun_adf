# decision_agent/decision_logic.py

import os
import logging
import json
import os.path
from shared.schemas import FailureContext
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_community.vectorstores import FAISS
from langchain.prompts import (
    ChatPromptTemplate,
    SystemMessagePromptTemplate,
    HumanMessagePromptTemplate,
)
from shared.utils import setup_logger
from dotenv import load_dotenv

load_dotenv()

logger = setup_logger("Decision_Logic_Agent")

class DecisionLogicAgent:
    def __init__(self):
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OPENAI_API_KEY is not set in environment variables.")

        self.chat = ChatOpenAI(model="gpt-4", temperature=0, openai_api_key=api_key)
        self.embeddings = OpenAIEmbeddings(openai_api_key=api_key)

        # ✅ Fixed: use folder path instead of .faiss file
        index_path = os.path.join(os.path.dirname(__file__), "../knowledge_base/vectorstore")
        self.vectorstore = FAISS.load_local(
            index_path,
            self.embeddings,
            allow_dangerous_deserialization=True
        )

        self.system_prompt = SystemMessagePromptTemplate.from_template(
            "You are an expert Azure Data Factory pipeline assistant. "
            "Analyze pipeline failure error messages and relevant documentation context to decide the retry action."
        )

        # ✅ Improved: added 'Return ONLY a valid JSON object' instruction
        self.human_template = """
        An Azure Data Factory pipeline has failed.
        Pipeline Name: {pipeline_name}
        Failed Activity: {failed_activity}
        Error Message: {error_message}
        Knowledge base context:
        {knowledge_text}
        
        Based on the error and knowledge base, decide:
        - full_rerun: rerun entire pipeline
        - partial_rerun: rerun from failed activity
        - no_rerun: do not retry, escalate to human

        Return ONLY a valid JSON object in this exact format:
        {{ "action": "full_rerun" | "partial_rerun" | "no_rerun", "reason": "..." }}
        """
        self.chat_prompt_template = ChatPromptTemplate.from_messages(
            [
                self.system_prompt,
                HumanMessagePromptTemplate.from_template(self.human_template),
            ]
        )

    def retrieve_knowledge(self, query: str, k=3) -> str:
        results = self.vectorstore.similarity_search(query, k=k)
        if not results:
            return ""
        combined_text = "\n\n".join([doc.page_content for doc in results])
        logger.debug(f"Retrieved knowledge for query '{query}' with {len(results)} documents.")
        return combined_text

    def make_decision(self, failure_context: FailureContext, max_retries=3, retry_delay=2):
        knowledge = self.retrieve_knowledge(failure_context.error_message).strip()
        failure_context.knowledge_text = knowledge if knowledge else "No relevant information found in knowledge base."

        prompt_vars = {
            "pipeline_name": failure_context.pipeline_name,
            "failed_activity": failure_context.failed_activity or "Unknown",
            "error_message": failure_context.error_message,
            "knowledge_text": failure_context.knowledge_text,
        }

        prompt = self.chat_prompt_template.format_prompt(**prompt_vars)
        logger.debug(f"Sending prompt to GPT-4:\n{prompt.to_string()}")

        attempt = 0
        while attempt < max_retries:
            try:
                response = self.chat.invoke(prompt.to_messages())
                logger.debug(f"Raw GPT response content: {response.content}")

                decision = json.loads(response.content)
                if "action" not in decision or "reason" not in decision:
                    raise ValueError("Decision JSON missing keys 'action' or 'reason'.")
                return decision
            except json.JSONDecodeError as e:
                logger.error(f"Unable to parse GPT response as JSON: {e}")
            except Exception as e:
                logger.error(f"Error during decision making: {e}")

            attempt += 1
            if attempt < max_retries:
                logger.info(f"Retrying GPT call (attempt {attempt + 1} of {max_retries}) after delay...")
                import time
                time.sleep(retry_delay)

        # Fallback decision if all retries failed
        return {
            "action": "no_rerun",
            "reason": "Failed to parse LLM response or encountered repeated errors",
        }
