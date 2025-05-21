# ORC File Merge

## Spark

### 


### Python

```python
from pyspark.sql import SparkSession

# Spark 세션 생성 (Hive 메타스토어 연동)
spark = SparkSession.builder \
    .appName("Merge ORC Files") \
    .enableHiveSupport() \
    .getOrCreate()

# Hive 외부 테이블명
table_name = "mydb.external_orc_table"

# 1. Hive 테이블의 HDFS 위치 조회
#    DESCRIBE FORMATTED 결과에서 Location 항목 추출
desc_df = spark.sql(f"DESCRIBE FORMATTED {table_name}")
location = (
    desc_df.filter(desc_df.col_name == "Location")
           .select("data_type")
           .first()[0]
)

# 2. 서브디렉토리 재귀 스캔 활성화
spark.conf.set("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", "true")

# 3. ORC 파일 로드 (HDFS 경로 기반)
df = spark.read.format("orc").load(location)

# 4. DataFrame 가공 (필터·컬럼 조정 등)
df_transformed = (
    df
    .filter("event_time >= '2025-01-01'")  # 예시
    .withColumnRenamed("old_col", "new_col")
    .select("new_col", "other_col", "event_time")
)

# 5. 단일 파일 병합을 위한 파티션 조정
df_single = df_transformed.coalesce(1)

# 6. HDFS 경로에 ORC로 저장
target_path = "hdfs:///merged_orc_output"
df_single.write.format("orc").mode("overwrite").save(target_path)

spark.stop()
```

```python
from pyspark.sql import SparkSession

# Spark 세션 생성 및 Hive, Iceberg 카탈로그 설정
spark = SparkSession.builder \
    .appName("Merge ORC and Write Iceberg Parquet") \
    .enableHiveSupport() \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.ice.type", "hadoop") \
    .config("spark.sql.catalog.ice.warehouse", "hdfs:///iceberg_warehouse") \
    .getOrCreate()

# Hive 테이블명
table_name = "mydb.external_orc_table"

# 1. Hive 테이블의 HDFS 경로 조회 (DESCRIBE FORMATTED 사용)
desc_df = spark.sql(f"DESCRIBE FORMATTED {table_name}")
location = (
    desc_df.filter(desc_df.col_name == "Location")
           .select("data_type")
           .first()[0]
)

# 2. 하위 디렉토리 재귀 스캔 설정 (필요한 경우)
spark.conf.set("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", "true")

# 3. 해당 경로의 ORC 파일 로드
#    서브디렉토리 포함하여 모든 ORC 파일을 읽음
df = spark.read.format("orc").load(f"{location}")

# 4. DataFrame 가공 (예: 필터링, 컬럼 추가 등)
df_transformed = (
    df
    .filter("event_time >= '2025-01-01'")
    .withColumnRenamed("old_col", "new_col")
    .select("new_col", "other_col", "event_time")
)

# 5. 병합을 위한 파티션 조정
df_single = df_transformed.coalesce(1)

# 6. Iceberg Parquet 테이블로 쓰기
(
    df_single
    .writeTo("ice.iceberg_db.merged_table")  # 'ice' 카탈로그 사용
    .using("iceberg")
    .tableProperty("write.format.default", "parquet")
    .createOrReplace()
)

spark.stop()
```

## References

* Online ORC File Viewer (https://dataconverter.io/)