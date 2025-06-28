from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col

spark = SparkSession.builder \
    .appName("Load and Merge CSVs") \
    .enableHiveSupport() \
    .getOrCreate()

# ============================================================
# 1. Read edx_courses.csv
# ============================================================
df_edx = (
    spark.read
        .option("header", True)
        .option("multiLine", True)
        .option("quote", "\"")
        .option("escape", "\"")
        .csv("hdfs://namenode:9000/user/root/input/edx_courses.csv")
)

# Bersihkan angka enrolled (hilangkan koma)
df_edx_clean = df_edx \
    .withColumn("n_enrolled", regexp_replace("n_enrolled", ",", "").cast("int")) \
    .dropDuplicates()

# Tulis ulang ke output cleaned → pakai delimiter ;
df_edx_clean.coalesce(1).write \
    .option("header", True) \
    .option("delimiter", ";") \
    .mode("overwrite") \
    .csv("hdfs://namenode:9000/user/root/output/processed_edx_courses.csv")

# ============================================================
# 2. Read coursera_data.csv
# ============================================================
df_coursera = (
    spark.read
        .option("header", True)
        .option("multiLine", True)
        .option("quote", "\"")
        .option("escape", "\"")
        .csv("hdfs://namenode:9000/user/root/input/coursera_data.csv")
)

# Hilangkan kolom sampah _c...
cols = [c for c in df_coursera.columns if c and not c.startswith("_c")]
df_coursera_clean = df_coursera.select(*cols)

# Bersihkan angka enrolled
df_coursera_clean = df_coursera_clean \
    .withColumn(
        "course_students_enrolled",
        regexp_replace("course_students_enrolled", "k", "000").cast("int")
    )

# Tulis ulang ke output cleaned → pakai delimiter ;
df_coursera_clean.coalesce(1).write \
    .option("header", True) \
    .option("delimiter", ";") \
    .mode("overwrite") \
    .csv("hdfs://namenode:9000/user/root/output/processed_coursera_data.csv")

# ============================================================
# 3. Create merged CSV with selected columns
# ============================================================

# Pilih kolom dari EDX
df_edx_sel = df_edx_clean.select(
    col("title").alias("course_title"),
    col("institution").alias("course_organization"),
    col("Level").alias("course_level")
)

# Pilih kolom dari Coursera
df_coursera_sel = df_coursera_clean.select(
    col("course_title"),
    col("course_organization"),
    col("course_difficulty").alias("course_level")
)

# Union keduanya
df_union = df_edx_sel.unionByName(df_coursera_sel)

# Tulis CSV hasil gabungan → pakai delimiter ;
df_union.coalesce(1).write \
    .option("header", True) \
    .option("delimiter", ";") \
    .mode("overwrite") \
    .csv("hdfs://namenode:9000/user/root/output/merged_courses.csv")

spark.stop()
