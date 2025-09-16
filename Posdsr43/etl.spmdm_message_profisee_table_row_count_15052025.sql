
from pyspark.sql.functions import col, concat, lit, length, substring

# Declare all variables as global
global execution_guid, package_name, proc_name, table_name, num_rows, message, message_type

# Assign default values (You might need to adjust based on your actual input)
execution_guid = None
package_name = None
proc_name = None
table_name = None
num_rows = None
message = ''
message_type = 'I'


query = f"""
SELECT i.rows
FROM profisee.sys.tables t
INNER JOIN profisee.sys.sysindexes i ON (t.object_id = i.id AND i.indid < 2)
INNER JOIN profisee.sys.schemas s ON t.schema_id = s.schema_id
WHERE ('[' + s.name + '].[' + t.NAME + ']') = '{table_name}'
OR (s.name + '.' + t.NAME) = '{table_name}'
"""
num_rows = spark.sql(query).collect()[0][0] if spark.sql(query).count() > 0 else None


if num_rows is None:
    message = f"Profisee Table: {table_name} not found"
    message_type = 'E'
else:
    message = f"Profisee Table: {table_name} Rows: {num_rows}"

# Assuming spmdm_message_insert is a pre-defined Spark function or table
# Adjust this based on your actual implementation for inserting messages
spark.sql(f"""
INSERT INTO etl.spmdm_message_insert VALUES ('{message}', '{message_type}', '{proc_name}', '{package_name}', '{execution_guid}')
""")

#End_DBShift
