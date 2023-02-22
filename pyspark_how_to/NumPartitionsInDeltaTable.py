# First Method:

# specify the Delta table name
table_name = "my_delta_table"

# execute a SQL query to retrieve the partition values
query = f"SHOW PARTITIONS {table_name}"
partitions = spark.sql(query)

# show the result
display(partitions.count())
#partitions.show()

## 2nd Method - Not to accurate

# set Delta table name
delta_table_name = "schema_name.table_name"

# get Delta table path
delta_table_path = spark.sql(f"DESCRIBE EXTENDED {delta_table_name}").filter("col_name = 'Location'").select("data_type").collect()[0][0]

# get number of partitions
num_partitions = spark.read.format("delta").option("path", delta_table_path).load().rdd.getNumPartitions()

# print number of partitions
print(f"Number of partitions in Delta table {delta_table_name}: {num_partitions}")
