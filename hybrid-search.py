import os
from langchain_community.document_loaders import CSVLoader
from langchain_qdrant import QdrantVectorStore, FastEmbedSparse, RetrievalMode
from langchain_openai import OpenAIEmbeddings
from langchain_community.vectorstores import Qdrant

# 현재 스크립트의 디렉토리 경로를 얻습니다
current_dir = os.path.dirname(os.path.abspath(__file__))

# CSV 파일의 전체 경로를 생성합니다
csv_path = os.path.join(current_dir, "data", "movies.csv")

# CSV 파일 로드
loader = CSVLoader(csv_path)
docs = loader.load()

print(f"로드된 문서 수: {len(docs)}")

open_ai_base_url = "https://api.platform.a15t.com/v1"
openai_api_key = "sk-ㅇㅇㅇ-s578qw"  # api key 
emb = OpenAIEmbeddings(
    deployment="text-embedding-3-large",
    model="openai/text-embedding-3-large",
    base_url=open_ai_base_url,
    api_key=openai_api_key
)

sparse_embeddings = FastEmbedSparse(model_name="Qdrant/bm25")

qdrant = QdrantVectorStore.from_documents(
    docs,
    embedding=emb,  # Dense 임베딩 사용
    sparse_embedding=sparse_embeddings,  # 희소 임베딩 사용
    location=":memory:",
    collection_name="movies",
    retrieval_mode=RetrievalMode.HYBRID  # 하이브리드 검색 모드
)

query = "액션 영화 추천해주세요"
found_docs = qdrant.similarity_search_with_relevance_scores(query)

# 각 문서의 메타데이터를 출력하여 구조를 확인합니다
for doc, score in found_docs:
    print(doc.metadata)  # 메타데이터 출력

# 평점(rating) 순으로 내림차순 정렬
sorted_docs = sorted(found_docs, key=lambda x: float(x[0].page_content.split('rating: ')[1].split('\n')[0]), reverse=True)

print("검색 결과:")
for doc, score in sorted_docs:
    print(f"제목: {doc.page_content}")
    print(f"메타데이터: {doc.metadata}")
    print(f"관련 점수: {score}")
    print("---")
