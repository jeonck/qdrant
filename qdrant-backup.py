import streamlit as st
import requests
import json
import pandas as pd
from requests_toolbelt.multipart.encoder import MultipartEncoder

# Qdrant API 기본 설정
QDRANT_HOST = "qdrant-dev.sample.com"
QDRANT_PORT = "443"
QDRANT_API_URL = f"https://{QDRANT_HOST}:{QDRANT_PORT}/collections"

st.title("Qdrant 벡터DB 백업 앱")

# 컬렉션 목록 가져오기
collection_response = requests.get(QDRANT_API_URL)
collection_list = []

if collection_response.status_code == 200:
    collections = json.loads(collection_response.text)
    if collections and 'result' in collections and 'collections' in collections['result']:
        for collection in collections['result']['collections']:
            collection_name = collection['name']
            collection_info_url = f"{QDRANT_API_URL}/{collection_name}"
            collection_info_response = requests.get(collection_info_url)
            if collection_info_response.status_code == 200:
                collection_info = json.loads(collection_info_response.text)
                if collection_info and 'result' in collection_info:
                    points_count = collection_info['result']['points_count'] if 'points_count' in collection_info['result'] else 0
                    collection_list.append(f"{collection_name} ({points_count} points)")
            else:
                st.warning(f"'{collection_name}'의 정보를 가져오는 중 오류 발생: {collection_info_response.status_code}")

        if len(collection_list) == 0:
            st.info("컬렉션이 존재하지 않습니다.")
    else:
        st.info("컬렉션이 존재하지 않습니다.")
else:
    st.error(f"컬렉션 목록을 가져오는 중 오류 발생: {collection_response.status_code} - {collection_response.text}")

# 1. 백업 생성 기능
st.header("백업 생성")
selected_collection = st.selectbox("백업할 컬렉션 선택", options=collection_list)
if st.button("백업 생성"):
    if selected_collection:
        collection_name = selected_collection.split(' ')[0]  # collection name 추출
        snapshot_url = f"https://{QDRANT_HOST}:{QDRANT_PORT}/collections/{collection_name}/snapshots"
        response = requests.post(snapshot_url)
        if response.status_code == 200:
            st.success("백업이 성공적으로 생성되었습니다!")
        else:
            st.error(f"백업 생성 실패: {response.status_code} - {response.text}")

# 2. 백업 목록 보기
st.header("백업 목록")
backup_names = []  # 백업 이름 목록
backup_data = []   # 백업 정보를 저장할 리스트

if selected_collection:
    collection_name = selected_collection.split(' ')[0]  # collection name 추출
    snapshot_url = f"https://{QDRANT_HOST}:{QDRANT_PORT}/collections/{collection_name}/snapshots"
    response = requests.get(snapshot_url)
    if response.status_code == 200:
        backups = response.json()
        if isinstance(backups, dict) and 'result' in backups and isinstance(backups['result'], list):
            snapshots = backups['result']
            if len(snapshots) > 0:
                for backup in snapshots:
                    backup_name = backup.get('name')
                    backup_size = backup.get('size')
                    created_at = backup.get('creation_time')

                    backup_names.append(backup_name)
                    backup_data.append({
                        "백업 이름": backup_name,
                        "크기 (bytes)": backup_size,
                        "생성일시": created_at,
                    })

                df = pd.DataFrame(backup_data)
                st.dataframe(df, hide_index=True)  # index를 숨기고 데이터프레임 출력
            else:
                st.info("백업이 존재하지 않습니다.")
        else:
            st.error("백업 데이터를 올바르게 가져오지 못했습니다.")
    else:
        st.error(f"백업 목록을 가져오는 중 오류 발생: {response.status_code} - {response.text}")

# 3. 백업 복원 기능
st.header("백업 복원")
backup_name_to_restore = st.selectbox("복원할 백업 선택", options=backup_names)
new_collection_name = st.text_input("새로운 컬렉션 이름 입력 (옵션)")

if st.button("복원"):
    if backup_name_to_restore:
        collection_name = selected_collection.split(' ')[0]
        restore_collection_name = new_collection_name if new_collection_name else collection_name
        
        snapshot_file_url = f"https://{QDRANT_HOST}:{QDRANT_PORT}/collections/{collection_name}/snapshots/{backup_name_to_restore}"
        
        try:
            # 스냅샷 파일을 가져오기
            response_snapshot = requests.get(snapshot_file_url, stream=True)
            if response_snapshot.status_code == 200:
                # 스냅샷 파일을 로컬에서 임시 파일로 저장
                temp_file_path = f"/tmp/{backup_name_to_restore}"
                with open(temp_file_path, 'wb') as f:
                    for chunk in response_snapshot.iter_content(chunk_size=8192):
                        if chunk: 
                            f.write(chunk)

                # 멀티파트 인코더를 사용하여 파일 업로드 설정
                m = MultipartEncoder(
                    fields={
                        'snapshot': (backup_name_to_restore, open(temp_file_path, 'rb'), 'application/octet-stream')
                    }
                )
                
                restore_url = f"https://{QDRANT_HOST}:{QDRANT_PORT}/collections/{restore_collection_name}/snapshots/upload?priority=snapshot"
                
                response = requests.post(
                    restore_url,
                    headers={
                        'Content-Type': m.content_type,
                        # 'api-key': API_KEY
                    },
                    data=m
                )
                
                if response.status_code == 200:
                    st.success(f"{backup_name_to_restore} 복원이 완료되었습니다!")
                else:
                    st.error(f"복원 실패: {response.status_code} - {response.text}")
                    st.write(f"응답 상태 코드: {response.status_code}")
                    st.write(f"응답 텍스트: {response.text}")
                    st.write(f"응답 헤더: {response.headers}")
                    
                    if response.text:
                        try:
                            json_response = response.json()
                            st.json(json_response)
                        except requests.exceptions.JSONDecodeError:
                            st.error("응답 본문을 JSON으로 디코딩할 수 없습니다.")
                    else:
                        st.error("응답 본문이 비어 있습니다.")
            else:
                st.error(f"스냅샷 파일을 가져오는 중 오류 발생: {response_snapshot.status_code} - {response_snapshot.text}")
        except Exception as e:
            st.error(f"스냅샷 파일을 가져오는데 실패했습니다: {str(e)}")
