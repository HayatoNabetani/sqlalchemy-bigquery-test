import concurrent.futures
import multiprocessing as multi
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


def get_app_query(word):
    s_time = time.time()
    db = SessionLocal()
    result = db.query(BigQueryApp).filter(BigQueryApp.name == word).all()
    e_time = time.time()
    print(f'?????? かかった時間:{e_time - s_time}秒 ???????')
    return result


if __name__ == '__main__':
    s_time = time.time()
    # 10個
    search_words = [
        "tiktok",
        "facebook",
        "instagram",
        "youtube",
        "shorts",
        "tiktok",
        "youtube",
        "pangle",
        "instagram",
        "facebook",
    ]
    # 並行処理
    with concurrent.futures.ThreadPoolExecutor(max_workers=multi.cpu_count()) as executor:
        cost_app_results_list = list(
            executor.map(get_app_query, search_words)
        )
    # かかった時間:2.3396999835968018秒 japan
    e_time = time.time()
    for result in cost_app_results_list:
        print(result)
    print(f'かかった時間:{e_time - s_time}秒')
