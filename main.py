import time

from sqlalchemy.engine import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy.dialects.mysql import TIMESTAMP as Timestamp


Base = declarative_base()
# /Users/hnabetani/Desktop/factbase/Python/bigquery-sqlachemy/main.py:8: MovedIn20Warning: Deprecated API features detected! These feature(s) are not compatible with SQLAlchemy 2.0. To prevent incompatible upgrades prior to updating applications, ensure requirements files are pinned to "sqlalchemy<2.0". Set environment variable SQLALCHEMY_WARN_20=1 to show all deprecation warnings.  Set environment variable SQLALCHEMY_SILENCE_UBER_WARNING=1 to silence this message. (Background on SQLAlchemy 2.0 at: https://sqlalche.me/e/b8d9)


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


db = SessionLocal()


def get_app_query(search_word):
    result = db.query(BigQueryApp).filter(BigQueryApp.name == search_word).all()
    return result


if __name__ == '__main__':
    start_time = time.time()

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
    for search_word in search_words:
        result = get_app_query(search_word)
        print(result)

    db.close()
    end_time = time.time()
    print(f'かかった時間:{end_time - start_time}秒')
    # かかった時間:17.77370810508728秒 us-centeral
    # かかった時間:12.446184873580933秒 japan
