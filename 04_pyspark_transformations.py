# Databricks notebook source
from pyspark.sql import functions as F
df = spark.read.format("csv")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .load("/Volumes/workspace/default/t15_practice/Data1/")
display(df)

# COMMAND ----------

df = df.dropDuplicates(["student_id"])

display(df)

# COMMAND ----------

df = df.replace("NULL", None ,subset=['age','attendance_percent'])

df = df.withColumn("attendance_percent", F.col("attendance_percent").cast("double"))

# COMMAND ----------

df = df.dropna(subset=['age'])
display(df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql import functions as F
window_spec = Window.partitionBy('department')
df = df.withColumn("attendance_percent" ,F.coalesce(F.col("attendance_percent"), F.round(F.avg("attendance_percent").over(window_spec),2)))
display(df)


# COMMAND ----------

df = df.replace("NULL",None,subset=['external_marks'])
df = df.withColumn('external_marks',F.col("external_marks").cast("double"))
df.display()

# COMMAND ----------

df = df.withColumn("external_marks",F.round(F.coalesce(F.col("external_marks"),F.avg("external_marks").over(window_spec))))
df.display()

# COMMAND ----------

df = df.replace("NULL",None,subset=['total_marks'])
df = df.withColumn('total_marks',F.col("total_marks").cast("double"))
df = df.withColumn("total_marks",F.round(F.coalesce(F.col("total_marks"),F.avg("total_marks").over(window_spec))))
df.display()

# COMMAND ----------

df = df.withColumn('student_name',F.initcap(F.col("student_name")))
df.display()

# COMMAND ----------

df_res = df.groupBy("department").agg(
    F.round(F.avg("total_marks")).alias("Avg_total_marks"),
    F.round(F.count("student_id")).alias("number_of_students"),
    F.round(F.avg("attendance_percent")).alias("Avg_attendance_percent")
)

df_res.display()


# COMMAND ----------

df.describe()

# COMMAND ----------

df.select("department").distinct().display()

# COMMAND ----------

df.filter(df.age > 24).count()

# COMMAND ----------

df.filter(df.age >19).display()

# COMMAND ----------

df.show()
df.printSchema()
df.columns
df.describe()