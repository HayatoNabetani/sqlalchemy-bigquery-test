from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy.dialects.mysql import TIMESTAMP as Timestamp
from sqlalchemy.schema import Table, MetaData, Column
from sqlalchemy.sql import expression, select, literal_column


# provide the path to a service account JSON file
engine = create_engine(
    'bigquery://',
    credentials_path='kashika-dpro-dev-0004.json'
)

table = Table(
    'kashika-dpro-dev-378004.dpro_test.apps',
    MetaData(bind=engine),  # /Users/hnabetani/Desktop/factbase/Python/bigquery-sqlachemy/main2.py:19: RemovedIn20Warning: Deprecated API features detected! These feature(s) are not compatible with SQLAlchemy 2.0. To prevent incompatible upgrades prior to updating applications, ensure requirements files are pinned to "sqlalchemy<2.0". Set environment variable SQLALCHEMY_WARN_20=1 to show all deprecation warnings.  Set environment variable SQLALCHEMY_SILENCE_UBER_WARNING=1 to silence this message. (Background on SQLAlchemy 2.0 at: https://sqlalche.me/e/b8d9)
    Column("id", Integer, autoincrement=True, primary_key=True, comment="媒体ID"),
    Column("name", String(length=20), nullable=False, unique=True, comment="媒体名"),
    Column("nickname", String(length=20), nullable=False, unique=True, comment="媒体名の略称"),
    Column("description", String(length=255), nullable=False, comment="媒体の説明"),
    Column(
        "created_at",
        Timestamp,
        nullable=False,
        server_default=text("current_timestamp"),
        comment="作成日時",
    ),
    Column(
        "updated_at",
        Timestamp,
        nullable=False,
        server_default=text("current_timestamp on update current_timestamp"),
        comment="更新日時",
    )
)

# 直接SQLを実行
result = select([table.c.id, table.c.name]).execute().fetchall()

# 結果を表示
for row in result:
    print(row.name)
