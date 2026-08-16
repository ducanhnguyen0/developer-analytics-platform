from pyspark.sql.types import StructType, StructField, StringType

GITHUB_EVENT_SCHEMA = StructType([
    StructField("id", StringType(), nullable=False),
    StructField("type", StringType(), nullable=False),
    StructField("actor_login", StringType(), nullable=True),
    StructField("repo_name", StringType(), nullable=True),
    StructField("created_at", StringType(), nullable=False)
])
