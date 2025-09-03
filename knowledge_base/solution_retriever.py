# knowledge_base/solution_retriever.py
import os
from langchain_community.vectorstores import FAISS
from langchain_openai import OpenAIEmbeddings, ChatOpenAI
from langchain.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from dotenv import load_dotenv

load_dotenv()

INDEX_DIR = os.path.join(os.path.dirname(__file__), "vectorstore")

PROMPT_TEMPLATE = """You are an Azure Data Factory troubleshooting assistant.
Given this failure reason from a pipeline run:
---
{failure_reason}
---
and the following retrieved documentation:
---
{context}
---
Provide a direct, actionable, step-by-step solution based ONLY on the above documentation.
If no relevant solution is in the documentation, reply exactly: 'No documented solution found.'
"""

class RAGSolutionRetriever:
    def __init__(self, top_k=3, model_name="gpt-4"):
        openai_api_key = os.getenv("OPENAI_API_KEY")
        if not openai_api_key:
            raise ValueError("OPENAI_API_KEY not set in environment")

        embeddings = OpenAIEmbeddings(openai_api_key=openai_api_key)
        self.vectorstore = FAISS.load_local(
            INDEX_DIR,
            embeddings,
            allow_dangerous_deserialization=True
        )
        self.retriever = self.vectorstore.as_retriever(search_kwargs={"k": top_k})
        self.llm = ChatOpenAI(model=model_name, temperature=0, openai_api_key=openai_api_key)
        self.prompt = PromptTemplate.from_template(PROMPT_TEMPLATE)
        self.parser = StrOutputParser()

    def get_solution(self, failure_reason: str) -> str:
        if not failure_reason.strip():
            return "No documented solution found."

        docs = self.retriever.invoke(failure_reason)
        if not docs:
            return "No documented solution found."

        context = "\n\n---\n\n".join(
            [f"Source: {d.metadata.get('source_file', 'Unknown')}\n{d.page_content}" for d in docs]
        )

        prompt_msg = self.prompt.invoke({"failure_reason": failure_reason, "context": context})
        response = self.llm.invoke(prompt_msg)
        return self.parser.invoke(response)
