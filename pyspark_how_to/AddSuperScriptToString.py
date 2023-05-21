# Here FULLTRAC will have super script TM like trademark

raw_df = spark.sql("select * from delta.`/mnt/edl/raw/table`")
raw_df = raw_df.withColumn("new_desc", regexp_replace(col("desc"), "\.", " ")) \
               .withColumn("new_desc", regexp_replace(col("new_desc"), "([a-z])([A-Z])", "$1 $2")) \
               .withColumn("new_desc", upper(col("new_desc"))) \
               .withColumn("new_desc", regexp_replace(col("new_desc"), "FULL TRAC", "FULLTRAC\u2122")) \
               .withColumn("new_desc", regexp_replace(col("new_desc"), "FULL CRUISE", "FULLCRUISE\u2122"))

raw_df = raw_df.drop("desc").withColumnRenamed("new_desc", "desc")
display(raw_df)
raw_df.createOrReplaceTempView("raw_vw")
