# 필요한 라이브러리 임포트
import streamlit as st
import asyncio
import pandas as pd
from langchain_openai import OpenAIEmbeddings
from langchain.schema import Document
from langchain.text_splitter import CharacterTextSplitter
from qdrant_client import QdrantClient
from qdrant_client.http.models import PointStruct, VectorParams, Distance
from concurrent.futures import ThreadPoolExecutor
import time

# 설정 및 상수
QDRANT_COLLECTION_NAME = "test240828"
QDRANT_URL = "https://qdrant-dev.sample.com:443"
QDRANT_API_KEY = ""
OPEN_AI_BASE_URL = "https://api.platform.a15t.com/v1"
OPENAI_API_KEY = "sk-ㅇㅇㅇ"
BATCH_SIZE = 100
MAX_POINTS_PER_BATCH = 50

# OpenAI 임베딩 모델 설정
emb = OpenAIEmbeddings(
    deployment="text-embedding-3-large",
    model="openai/text-embedding-3-large",
    base_url=OPEN_AI_BASE_URL,
    api_key=OPENAI_API_KEY
)

# Qdrant 클라이언트 설정
qdrant_client = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY)

# 텍스트 분할기 설정
text_splitter = CharacterTextSplitter(
    separator="\n",
    chunk_size=100,
    chunk_overlap=5
)

def process_file(uploaded_file, doc_id):
    """
    업로드된 파일을 처리하여 텍스트와 메타데이터로 변환합니다.
    CSV 파일과 일반 텍스트 파일을 구분하여 처리합니다.
    """
    texts = []
    metadata = []
    
    if uploaded_file.name.endswith('.csv'):
        df = pd.read_csv(uploaded_file)
        for _, row in df.iterrows():
            raw_text = " ".join(map(str, row.values))
            process_text(raw_text, uploaded_file.name, doc_id, texts, metadata)
    else:
        raw_text = uploaded_file.getvalue().decode("utf-8")
        process_text(raw_text, uploaded_file.name, doc_id, texts, metadata)
    
    return texts, metadata, doc_id

def process_text(raw_text, filename, doc_id, texts, metadata):
    """
    텍스트를 처리하여 청크로 분할하고 메타데이터를 생성합니다.
    """
    document = Document(page_content=raw_text)
    documents = text_splitter.split_documents([document])
    for doc in documents:
        texts.append(doc.page_content)
        metadata.append({"doc_id": doc_id, "filename": filename, "text": doc.page_content})
        doc_id += 1
    return doc_id

def embed_batch(batch_texts):
    """
    텍스트 배치를 임베딩합니다.
    """
    return emb.embed_documents(batch_texts)

def upsert_batch(embeddings, metadata, start, end):
    """
    임베딩과 메타데이터 배치를 Qdrant에 업로드합니다.
    """
    batch_points = [
        PointStruct(id=j, vector=embeddings[j], payload=metadata[j])
        for j in range(start, end)
    ]
    qdrant_client.upsert(
        collection_name=QDRANT_COLLECTION_NAME,
        points=batch_points
    )

async def save_rag(uploaded_files):
    """
    업로드된 파일들을 처리하고 Qdrant에 저장하는 메인 함수입니다.
    """
    start_time = time.time()
    
    texts = []
    metadata = []
    doc_id = 0

    # 파일 처리
    for uploaded_file in uploaded_files:
        file_texts, file_metadata, doc_id = process_file(uploaded_file, doc_id)
        texts.extend(file_texts)
        metadata.extend(file_metadata)

    # 임베딩 처리
    embeddings = process_embeddings(texts)
    embedding_time = time.time() - start_time
    
    # Qdrant 컬렉션 설정 및 데이터 업로드
    setup_qdrant_collection(len(embeddings[0]))
    upload_to_qdrant(embeddings, metadata)

    total_time = time.time() - start_time
    return embedding_time, total_time

def process_embeddings(texts):
    """
    텍스트를 병렬로 임베딩 처리합니다.
    """
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(embed_batch, texts[i:i + BATCH_SIZE]) 
                   for i in range(0, len(texts), BATCH_SIZE)]
        return [emb for future in futures for emb in future.result()]

def setup_qdrant_collection(embeddings_size):
    """
    Qdrant 컬렉션을 설정합니다. 이미 존재하는 경우 삭제 후 재생성합니다.
    """
    if qdrant_client.collection_exists(QDRANT_COLLECTION_NAME):
        qdrant_client.delete_collection(QDRANT_COLLECTION_NAME)
    qdrant_client.create_collection(
        collection_name=QDRANT_COLLECTION_NAME,
        vectors_config=VectorParams(size=embeddings_size, distance=Distance.COSINE)
    )

def upload_to_qdrant(embeddings, metadata):
    """
    임베딩과 메타데이터를 병렬로 Qdrant에 업로드합니다.
    """
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(upsert_batch, embeddings, metadata, i, 
                                   min(i + MAX_POINTS_PER_BATCH, len(embeddings))) 
                   for i in range(0, len(embeddings), MAX_POINTS_PER_BATCH)]
        for future in futures:
            future.result()

async def retrive_rag(question):
    """
    주어진 질문에 대해 Qdrant에서 유사한 문서를 검색합니다.
    """
    # 질문을 임베딩으로 변환
    query_embedding = emb.embed_query(question)

    # Qdrant에서 유사한 문서 검색
    result = qdrant_client.search(
        collection_name=QDRANT_COLLECTION_NAME,
        query_vector=query_embedding,
        limit=5
    )

    return result

def main():
    """
    Streamlit 앱의 메인 함수입니다.
    파일 업로드, 임베딩 저장, 질문 검색 기능을 제공합니다.
    """
    st.title("Qdrant와 OpenAI를 이용한 문서 검색")

    # 파일 업로드 UI
    uploaded_files = st.file_uploader("파일을 업로드하세요", accept_multiple_files=True, type=["txt", "csv"])

    if uploaded_files:
        if st.button("파일 임베딩 및 저장"):
            embedding_time, total_time = asyncio.run(save_rag(uploaded_files))
            st.success("파일이 성공적으로 임베딩되고 저장되었습니다.")
            st.info(f"임베딩 소요 시간: {embedding_time:.2f}초")
            st.info(f"전체 처리 시간: {total_time:.2f}초")

    # 질문 입력 UI
    question = st.text_input("질문을 입력하세요")

    if question:
        if st.button("검색"):
            contexts = asyncio.run(retrive_rag(question))
            st.write("검색 결과:")
            for context in contexts:
                st.write(f"id: {context.id}")
                st.write(f"score: {context.score}")
                st.write(f"text: {context.payload['text']}")
                st.write("---")

if __name__ == "__main__":
    main()
