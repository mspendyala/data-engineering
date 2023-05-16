from pyspark.sql.functions import col, substring
from pyspark.sql.types import IntegerType

# First 2 digits of mach_id
add_part_key_first_2_df = raw_df_only_mach_id.withColumn("part_key_first_2", substring(col("mach_id"),1,2))
add_part_key_first_2_int_df = add_part_key_first_2_df.withColumn("part_key_first_2", add_part_key_first_2_df.part_key_first_2.cast(IntegerType()))
