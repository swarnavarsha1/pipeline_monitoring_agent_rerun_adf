# knowledge_base/build_index.py

import os
from langchain_community.document_loaders import PyPDFLoader
from langchain_openai import OpenAIEmbeddings
from langchain_community.vectorstores import FAISS
from langchain.text_splitter import RecursiveCharacterTextSplitter
from dotenv import load_dotenv

load_dotenv() 

def build_pdf_index(pdf_path: str, index_path: str):
    loader = PyPDFLoader(pdf_path)
    documents = loader.load()

    text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=100)
    docs = text_splitter.split_documents(documents)

    openai_api_key = os.getenv("OPENAI_API_KEY")
    if not openai_api_key:
        raise ValueError("OPENAI_API_KEY not found in environment.")

    embeddings = OpenAIEmbeddings(openai_api_key=openai_api_key)

    vectorstore = FAISS.from_documents(docs, embeddings)

    # ✅ Save index as a folder, not a single .faiss file
    vectorstore.save_local(index_path)
    print(f"Index saved to {index_path}")

if __name__ == "__main__":
    pdf_file = os.path.join(os.path.dirname(__file__), "data.pdf")

    # ✅ Save to a folder called "vectorstore"
    index_dir = os.path.join(os.path.dirname(__file__), "vectorstore")
    build_pdf_index(pdf_file, index_dir)
