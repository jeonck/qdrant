import os
import time
from langchain_community.document_loaders import CSVLoader
from langchain_qdrant import FastEmbedSparse, QdrantVectorStore, RetrievalMode
from qdrant_client import QdrantClient
from qdrant_client.http import models as rest
from langchain_openai import OpenAIEmbeddings
 
# 현재 스크립트의 디렉토리 경로를 얻습니다
current_dir = os.path.dirname(os.path.abspath(__file__))
 
# CSV 파일의 전체 경로를 생성합니다
csv_path = os.path.join(current_dir, "data", "movies.csv")
 
# CSV 파일 로드
loader = CSVLoader(csv_path)
docs = loader.load()
 
print(f"로드된 문서 수: {len(docs)}")
 
# Qdrant 클라이언트 설정
client = QdrantClient(url="https://qdrant-dev.sample.com:443")
 
# 일반 임베딩 설정
open_ai_base_url = "https://api.platform.a15t.com/v1"
openai_api_key = "sk-ㅇㅇㅇㅇ-s578qw"
emb = OpenAIEmbeddings(
    deployment="text-embedding-3-large",
    model="openai/text-embedding-3-large",
    base_url=open_ai_base_url,
    api_key=openai_api_key
)
 
# 컬렉션 이름 설정
collection_name = "movies_dense"
 
# 임베딩 시작 시간 기록
embedding_start_time = time.time()
 
# Qdrant 벡터 저장소 생성 (Dense mode)
qdrant_dense = QdrantVectorStore.from_documents(
    docs,
    embedding=emb,
    url="https://qdrant-dev.sample.com:443",
    collection_name=collection_name,
    retrieval_mode=RetrievalMode.DENSE,
    force_recreate=True,  # 컬렉션 강제 재생성
)
 
# 임베딩 종료 시간 기록
embedding_end_time = time.time()
 
# 임베딩 소요 시간 계산
embedding_duration = embedding_end_time - embedding_start_time
print(f"임베딩 소요 시간: {embedding_duration:.2f} 초")
 
query = "액션 영화 추천해주세요"
found_docs = qdrant_dense.similarity_search_with_score(query)
 
print("검색 결과:")
for doc, score in found_docs:
    print(f"제목: {doc.page_content}")
    print(f"메타데이터: {doc.metadata}")
    print(f"점수: {score}")
    print("---")
