from pymongo import MongoClient
import pandas as pd

def load_csv_to_mongodb(csv_path, mongo_uri, db_name, collection_name):
    """
    CSV 파일을 읽어 MongoDB에 저장하는 함수.
    Args:
        csv_path (str): CSV 파일 경로
        mongo_uri (str): MongoDB 접속 URI
        db_name (str): 사용할 데이터베이스 이름
        collection_name (str): 사용할 컬렉션 이름
    """
    # 1) CSV 읽기
    df = pd.read_csv(csv_path, encoding='utf-8-sig')

    # 2) MongoDB 연결
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]

    # 3) DataFrame을 dict 리스트로 변환
    records = df.to_dict(orient='records')

    # 4) MongoDB에 Bulk Insert
    result = collection.insert_many(records)
    print(f"Inserted {len(result.inserted_ids)} documents into {db_name}.{collection_name}")

    # 5) 연결 종료
    client.close()


if __name__ == '__main__':
    # MongoDB 접속 정보
    user = 'opraipb'
    password = 'gmsh_opraipb'
    host = '10.196.10.30'
    port = '27017'
    db_name = 'edgar'
    collection_name = '8k_filing_raw'

    # URI 형식: mongodb://username:password@host:port/
    mongo_uri = f"mongodb://{user}:{password}@{host}:{port}/"

    # CSV 파일 경로
    csv_path = '8k_filings_raw.csv'

    load_csv_to_mongodb(csv_path, mongo_uri, db_name, collection_name)