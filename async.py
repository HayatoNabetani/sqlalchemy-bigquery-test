import asyncio
import time

from sqlalchemy.engine import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy.dialects.mysql import TIMESTAMP as Timestamp


Base = declarative_base()


class BigQueryApp(Base):
    # __tablename__ = "kashika-dpro-dev-378004.dpro_test.apps"
    __tablename__ = "apps"
    __table_args__ = {"comment": "媒体"}
    id = Column(Integer, autoincrement=True, primary_key=True, comment="媒体ID")
    name = Column("name", String(length=20), nullable=False, unique=True, comment="媒体名")
    nickname = Column("nickname", String(length=20), nullable=False, unique=True, comment="媒体名の略称")
    description = Column("description", String(length=255), nullable=False, comment="媒体の説明")
    created_at = Column(
        Timestamp,
        nullable=False,
        server_default=text("current_timestamp"),
        comment="作成日時",
    )
    updated_at = Column(
        Timestamp,
        nullable=False,
        server_default=text("current_timestamp on update current_timestamp"),
        comment="更新日時",
    )


# provide the path to a service account JSON file
bigquery_engine = create_engine(
    'bigquery://kashika-dpro-dev-378004/dpro_development',
    credentials_path='kashika-dpro-dev-0004.json'
)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=bigquery_engine
)


async def get_app_query(word):
    s_time = time.time()
    try:
        # 非同期実行のためにrun_in_executorを使用
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            lambda: fetch_query(word)
        )
        e_time = time.time()
        print(f'クエリ "{word}" にかかった時間: {e_time - s_time}秒')
        return result
    except Exception as e:
        print(f"クエリ実行中にエラーが発生しました: {e}")
        return None


# クエリ実行のためのヘルパー関数
def fetch_query(word):
    with SessionLocal() as db:
        return db.query(BigQueryApp).filter(BigQueryApp.name == word).all()


# メインの非同期関数
async def main():
    s_time = time.time()
    search_words = ["tiktok", "facebook", "instagram", "youtube", "shorts", "tiktok", "youtube", "pangle", "instagram", "facebook"]

    tasks = [get_app_query(word) for word in search_words]
    cost_app_results_list = await asyncio.gather(*tasks)
    for result in cost_app_results_list:
        print(result)
    e_time = time.time()
    print(f"TOTAL: {e_time - s_time}秒")

if __name__ == "__main__":
    asyncio.run(main())


# if __name__ == "__main__":
#     asyncio.run(main())
#     # TOTAL: >>>>>8.4551100730896秒>>>>>
