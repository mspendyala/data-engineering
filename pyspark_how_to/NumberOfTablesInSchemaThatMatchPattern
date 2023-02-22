# specify the schema name and the pattern to search for
schema_name = "schema_name"
table_pattern = "table_pattern_"

# get a list of tables in the schema
table_list = spark.catalog.listTables(schema_name)

# filter the list to tables that match the pattern
matching_tables = [table.name for table in table_list if table.name.startswith(table_pattern)]

# get the count of matching tables
table_count = len(matching_tables)

# print the result
print(f"There are {table_count} tables in the {schema_name} schema that match the pattern {table_pattern}")
