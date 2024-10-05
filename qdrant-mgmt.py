import streamlit as st
import requests
import pandas as pd
from qdrant_client import QdrantClient
from qdrant_client.http import models
from datetime import datetime
import time
 
# Qdrant 서버의 URL
base_url = "https://qdrant-dev.sample.com:443/collections"
 
# Qdrant 클라이언트 초기화 (이 부분은 파일 상단에 위치해야 합니다)
client = QdrantClient("https://qdrant-dev.sample.com:443")
 
# 세션 상태 초기화
if 'tab1_state' not in st.session_state:
    st.session_state.tab1_state = {
        'collections': [],
        'refresh_collections': True
    }
 
if 'tab2_state' not in st.session_state:
    st.session_state.tab2_state = {
        'aliases': [],
        'refresh_aliases': True
    }
 
# 모든 컬렉션 목록과 크기를 가져오는 함수
def get_all_collections():
    response = requests.get(base_url)
    if response.status_code == 200:
        collections = response.json().get("result", {}).get("collections", [])
        collection_info = []
        for collection in collections:
            name = collection["name"]
            size_response = requests.get(f"{base_url}/{name}")
            if size_response.status_code == 200:
                result = size_response.json().get("result", {})
                vector_size = result.get("config", {}).get("params", {}).get("vectors", {}).get("size", 0)
                points_count = result.get("points_count", 0)
                bytes_per_float = 4  # float32는 4바이트
                total_size_in_bytes = vector_size * points_count * bytes_per_float
                total_size_in_mb = total_size_in_bytes / (1024 * 1024)
                collection_info.append({
                    "이름": name,
                    "벡터 크기": vector_size,
                    "포인트 수": points_count,
                    "데이터 크기 (MB)": round(total_size_in_mb, 2)
                })
            else:
                collection_info.append({
                    "이름": name,
                    "벡터 크기": "N/A",
                    "포인트 수": "N/A",
                    "데이터 크기 (MB)": "N/A"
                })
        return collection_info
    else:
        st.error(f"컬렉션 목록 가져오기 실패: {response.status_code}, {response.text}")
        return []
 
# 컬렉션 확인 요청 함수
def check_collection(collection_name):
    qdrant_url = f"{base_url}/{collection_name}"
    response = requests.get(qdrant_url)
 
    if response.status_code == 200:
        collection_info = response.json()
        st.write(f"컬렉션 '{collection_name}'의 세부 정보:")
         
        # JSON을 DataFrame으로 변환하고 전치(transpose)
        df = pd.json_normalize(collection_info).T
         
        # 인덱스 이름 설정
        df.index.name = "속성"
        df.columns = ["값"]
         
        # 모든 값을 문자열로 변환
        df["값"] = df["값"].astype(str)
         
        # DataFrame 표시
        st.dataframe(
            df,
            use_container_width=True,
            column_config={
                "속성": st.column_config.TextColumn("속성", width="medium"),
                "값": st.column_config.TextColumn("값", width=10),
            }
        )
    else:
        st.error(f"컬렉션 조회 실패: {response.status_code}, {response.text}")
 
# 컬렉션 존재 여부 확인 함수
def collection_exists(collection_name):
    qdrant_url = f"{base_url}/{collection_name}"
    response = requests.get(qdrant_url)
    return response.status_code == 200
 
# 컬렉션 삭제 요청 함수
def delete_collection(collection_name):
    if not collection_exists(collection_name):
        st.warning(f"컬렉션 '{collection_name}'이(가) 존재하지 않습니다.")
        return
 
    qdrant_url = f"{base_url}/{collection_name}"
    response = requests.delete(qdrant_url)
    if response.status_code == 200:
        st.success(f"컬렉션 '{collection_name}'이(가) 성공적으로 삭제되었습니다!")
        time.sleep(1)  # 1초 대기
        st.session_state.tab1_state['refresh_collections'] = True
        st.rerun()
    else:
        st.error(f"컬렉션 삭제 실패: {response.status_code}, {response.text}")
 
# alias 목록을 가져오는 함수
def get_all_aliases():
    try:
        aliases_response = client.get_aliases()
        result = []
        for alias in aliases_response.aliases:
            result.append({"collection_name": alias.collection_name, "alias_name": alias.alias_name})
        return result
    except Exception as e:
        st.error(f"Alias 목록 가져오기 실패: {str(e)}")
        return []
 
# 탭 구성
tab1, tab2 = st.tabs(["컬렉션 관리", "Alias 관리"])
 
with tab1:
    st.header("컬렉션 관리")
 
    # 컬렉션 목록 갱신 버튼 추가
    if st.button("컬렉션 목록 새로고침"):
        st.session_state.tab1_state['refresh_collections'] = True
 
    # 컬렉션 목록 갱신이 필요한 경우
    if st.session_state.tab1_state['refresh_collections']:
        st.session_state.tab1_state['collections'] = get_all_collections()
        st.session_state.tab1_state['refresh_collections'] = False
 
    # 현재 컬렉션 목록 표시
    collection_list = st.session_state.tab1_state['collections']
    if collection_list:
        st.subheader("현재 컬렉션 목록")
        collection_df = pd.DataFrame(collection_list)
        st.dataframe(
            collection_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "이름": st.column_config.TextColumn("컬렉션 이름", width="medium"),
                "벡터 크기": st.column_config.NumberColumn("벡터 크기", width="small"),
                "포인트 수": st.column_config.NumberColumn("포인트 수", width="small"),
                "데이터 크기 (MB)": st.column_config.NumberColumn("데이터 크기 (MB)", width="small", format="%.2f"),
            }
        )
    else:
        st.info("현재 등록된 컬렉션이 없습니다.")
 
    st.markdown("---")  # 구분선 추가
 
    # 컬렉션 상세 조회 기능
    st.subheader("컬렉션 상세 조회")
    if collection_list:
        collection_name_view = st.selectbox("조회할 컬렉션 선택:", [c["이름"] for c in collection_list], key="view_collection")
        if collection_name_view:
            if st.button("컬렉션 상세 조회"):
                check_collection(collection_name_view)
    else:
        st.warning("조회할 수 있는 컬렉션이 없습니다.")
 
    st.markdown("---")  # 구분선 추가
 
    # 컬렉션 삭제 기능
    st.subheader("컬렉션 삭제")
 
    collection_name = st.text_input("삭제할 컬렉션 이름:", "example_collection")
    qdrant_url = f"{base_url}/{collection_name}"
 
 
    # 컬렉션 삭제 버튼
    if st.button("컬렉션 삭제"):
        delete_collection(collection_name)
 
with tab2:
    st.header("Alias 관리")
 
    # Alias 목록을 가져오고 세션 상태를 업데이트하는 함수
    def update_aliases():
        st.session_state.tab2_state['aliases'] = get_all_aliases()
        st.session_state.tab2_state['refresh_aliases'] = False
 
    # 탭의 상태를 관리하기 위한 세션 상태 초기화
    if 'tab2_state' not in st.session_state:
        st.session_state.tab2_state = {
            'aliases': [],
            'refresh_aliases': True
        }
 
    # Alias 목록이 갱신되어야 하는 경우 업데이트
    if st.session_state.tab2_state['refresh_aliases']:
        update_aliases()
 
    # Alias 목록 표시
    if st.session_state.tab2_state['aliases']:
        st.subheader("현재 Alias 목록")
        alias_df = pd.DataFrame(st.session_state.tab2_state['aliases'])
        alias_df.columns = ['컬렉션 이름', 'Alias 이름']
        st.dataframe(
            alias_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "컬렉션 이름": st.column_config.TextColumn("컬렉션 이름", width="medium"),
                "Alias 이름": st.column_config.TextColumn("Alias 이름", width="medium"),
            }
        )
    else:
        st.info("현재 등록된 Alias가 없습니다.")
 
    st.markdown("---")  # 구분선 추가
 
    # Alias 생성 기능
    st.subheader("Alias 생성")
    collection_list = get_all_collections()
    if collection_list:
        col1, col2 = st.columns(2)
        with col1:
            collection_name_for_alias = st.selectbox("Alias를 지정할 컬렉션 선택:", [c["이름"] for c in collection_list])
         
        with col2:
            default_alias_name = f"alias{datetime.now().strftime('%y%m%d%H')}_{collection_name_for_alias}"
            alias_name = st.text_input("새로운 Alias 이름:", default_alias_name)
 
        if st.button("Alias 생성"):
            try:
                client.update_collection_aliases(
                    change_aliases_operations=[
                        models.CreateAliasOperation(
                            create_alias=models.CreateAlias(
                                collection_name=collection_name_for_alias,
                                alias_name=alias_name
                            )
                        )
                    ]
                )
                st.success(f"Alias '{alias_name}'이(가) 컬렉션 '{collection_name_for_alias}'에 성공적으로 생성되었습니다!")
                st.session_state.tab2_state['refresh_aliases'] = True
                st.rerun()
            except Exception as e:
                st.error(f"Alias 생성 실패: {str(e)}")
    else:
        st.warning("사용 가능한 컬렉션이 없습니다. Alias를 생성하려면 먼저 컬렉션을 만드세요.")
 
    st.markdown("---")  # 구분선 추가
 
    # Alias 삭제 기능
    st.subheader("Alias 삭제")
    if st.session_state.tab2_state['aliases']:
        alias_to_delete = st.selectbox("삭제할 Alias 선택:", [a['alias_name'] for a in st.session_state.tab2_state['aliases']])
        if st.button("Alias 삭제"):
            try:
                client.update_collection_aliases(
                    change_aliases_operations=[
                        models.DeleteAliasOperation(
                            delete_alias=models.DeleteAlias(
                                alias_name=alias_to_delete
                            )
                        )
                    ]
                )
                st.success(f"Alias '{alias_to_delete}'이(가) 성공적으로 삭제되었습니다!")
                time.sleep(1)  # 1초 대기
                st.session_state.tab2_state['refresh_aliases'] = True
                st.rerun()
            except Exception as e:
                st.error(f"Alias 삭제 실패: {str(e)}")
    else:
        st.info("삭제할 Alias가 없습니다.")
 
    st.markdown("---")  # 구분선 추가
 
    # Alias 스위칭 기능
    st.subheader("Alias 스위칭")
    if st.session_state.tab2_state['aliases']:
        col1, col2 = st.columns(2)
        with col1:
            alias_name = st.selectbox("스위칭할 Alias 선택:", [a['alias_name'] for a in st.session_state.tab2_state['aliases']])
        with col2:
            collection_list = get_all_collections()
            new_collection_name = st.selectbox("새로운 컬렉션 선택:", [c["이름"] for c in collection_list])
         
        if st.button("Alias 스위칭"):
            alias_url = f"{base_url}/aliases"
            switch_data = {
                "actions": [
                    {
                        "delete_alias": {
                            "alias_name": alias_name
                        }
                    },
                    {
                        "create_alias": {
                            "alias_name": alias_name,
                            "collection_name": new_collection_name
                        }
                    }
                ]
            }
 
            response = requests.post(alias_url, json=switch_data)
            st.write(f"Alias 스위칭 요청 응답 코드: {response.status_code}")
            st.write(f"응답 내용: {response.text}")
            if response.status_code == 200:
                st.success(f"Alias '{alias_name}'이(가) 컬렉션 '{new_collection_name}'으로 성공적으로 스위칭되었습니다!")
                st.session_state.tab2_state['refresh_aliases'] = True
                st.rerun()
            else:
                st.error(f"Alias 스위칭 실패: {response.status_code}, {response.text}")
    else:
        st.info("스위칭할 Alias가 없습니다.")
 
    # Alias 생성, 삭제 또는 스위칭 후 상태 업데이트
    if st.session_state.tab2_state['refresh_aliases']:
        st.rerun()
