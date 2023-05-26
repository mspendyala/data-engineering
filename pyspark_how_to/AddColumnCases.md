from pyspark.sql.functions import col, substring
from pyspark.sql.types import IntegerType


# AddColumn_Cast_String_Integer.
## First 2 digits of mach_id
add_part_key_first_2_df = raw_df_only_mach_id.withColumn("part_key_first_2", substring(col("mach_id"),1,2))
add_part_key_first_2_int_df = add_part_key_first_2_df.withColumn("part_key_first_2", add_part_key_first_2_df.part_key_first_2.cast(IntegerType()))

# Add column based on a date expression

### Get the current date
current_date_value = current_date()

### Calculate the target date for filtering
target_date = current_date_value - expr("INTERVAL 8 DAYS")
Note: You cannot directly print the value of target_date. It must be added to a dataframe as a column and displayed

### Example target_date = 2023-05-18
df = df.withColumn("target_date", target_date)

display(df)
