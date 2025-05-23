# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5fc5a0e6-0257-4e0c-ba7c-9a2d5cb23285",
# META       "default_lakehouse_name": "lakehouse_streaming_delta",
# META       "default_lakehouse_workspace_id": "8bfcedba-edeb-4a23-9465-bbad5c8a3409",
# META       "known_lakehouses": [
# META         {
# META           "id": "5fc5a0e6-0257-4e0c-ba7c-9a2d5cb23285"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from notebookutils import mssparkutils
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Create a folder
inputPath = 'Files/data/'
mssparkutils.fs.mkdirs(inputPath)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.option("multiline", "true").json("Files/eventdata_output/invalid/part-00000-3ab14cea-36ad-41ad-9858-526534e3954b-c000.json")
# df now is a Spark DataFrame containing JSON data from "Files/eventdata_output/invalid/part-00000-3ab14cea-36ad-41ad-9858-526534e3954b-c000.json".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.option("multiline", "true").json("Files/eventdata_output/invalid/part-00000-3ab14cea-36ad-41ad-9858-526534e3954b-c000.json")
# df now is a Spark DataFrame containing JSON data from "Files/eventdata_output/invalid/part-00000-3ab14cea-36ad-41ad-9858-526534e3954b-c000.json".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.option("multiline", "true").json("Files/eventdata_output/valid/part-00000-cc03152b-be52-4bb9-a9c0-fc07d307ca25-c000.json")
# df now is a Spark DataFrame containing JSON data from "Files/eventdata_output/valid/part-00000-cc03152b-be52-4bb9-a9c0-fc07d307ca25-c000.json".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create a stream that reads data from the folder, using a JSON schema
jsonSchema = StructType([
StructField("device", StringType(), False),
StructField("status", StringType(), False)
])
iotstream = spark.readStream.schema(jsonSchema).option("maxFilesPerTrigger", 1).json(inputPath)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write some event data to the folder
device_data = '''{"device":"Dev1","status":"ok"}
{"device":"Dev1","status":"ok"}
{"device":"Dev1","status":"ok"}
{"device":"Dev2","status":"error"}
{"device":"Dev1","status":"ok"}
{"device":"Dev1","status":"error"}
{"device":"Dev2","status":"ok"}
{"device":"Dev2","status":"error"}
{"device":"Dev1","status":"ok"}'''

mssparkutils.fs.put(inputPath + "data.txt", device_data, True)

print("Source stream created...")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write the stream to a delta table
delta_stream_table_path = 'Tables/iotdevicedata'
checkpointpath = 'Files/delta/checkpoint'
deltastream = iotstream.writeStream.format("delta").option("checkpointLocation", checkpointpath).start(delta_stream_table_path)
print("Streaming to delta sink...")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM IotDeviceData;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add more data to the source stream
more_data = '''{"device":"Dev1","status":"ok"}
 {"device":"Dev1","status":"ok"}
 {"device":"Dev1","status":"ok"}
 {"device":"Dev1","status":"ok"}
 {"device":"Dev1","status":"error"}
 {"device":"Dev2","status":"error"}
 {"device":"Dev1","status":"ok"}'''

mssparkutils.fs.put(inputPath + "more-data.txt", more_data, True)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deltastream.stop()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define the input path
inputPath = "Files/eventdata/"

# Create the folder if it doesn't exist
mssparkutils.fs.mkdirs(inputPath)

# Device data with 2 bad rows
device_data = '''{"device":"Dev1","status":"ok"}
{"device":"Dev1","status":"ok"}
BAD_LINE_MISSING_JSON
{"device":"Dev1","status":"ok"}
{"device":"Dev2","status":"error"}
{"device":"Dev1","status":"ok"}
{"device":"Dev1","status":"error"}
{bad_json:123}
{"device":"Dev2","status":"ok"}
{"device":"Dev2","status":"error"}
{"device":"Dev1","status":"ok"}'''

# Write to a text file in the specified path
mssparkutils.fs.put(inputPath + "data.txt", device_data, True)

print("Source stream created...")

# Read and parse data line by line with error handling
from pyspark.sql import Row
import json

# Read file as plain text
raw_df = spark.read.text(inputPath + "data.txt")

# Parse each row with JSON safely
def safe_parse(row):
    try:
        return Row(**json.loads(row.text))
    except Exception as e:
        return Row(device=None, status=None, error=row.text)

# Apply the safe parsing
parsed_rdd = raw_df.rdd.map(safe_parse)

# Convert back to DataFrame
parsed_df = spark.createDataFrame(parsed_rdd)

# Show the result
parsed_df.show(truncate=False)

# Optionally, filter out bad rows for further processing
valid_data_df = parsed_df.filter("device IS NOT NULL AND status IS NOT NULL")
invalid_data_df = parsed_df.filter("device IS NULL AND status IS NULL")

print("✅ Valid rows:")
valid_data_df.show(truncate=False)

print("⚠️ Invalid rows (bad JSON):")
invalid_data_df.show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import Row
import json

# Define the input path
inputPath = "Files/eventdata/"
mssparkutils.fs.mkdirs(inputPath)

# Device data including malformed (bad) rows
device_data = '''{"device":"Dev1","status":"ok"}
{"device":"Dev1","status":"ok"}
BAD_LINE_MISSING_JSON
{"device":"Dev1","status":"ok"}
{"device":"Dev2","status":"error"}
{"device":"Dev1","status":"ok"}
{"device":"Dev1","status":"error"}
{bad_json:123}
{"device":"Dev2","status":"ok"}
{"device":"Dev2","status":"error"}
{"device":"Dev1","status":"ok"}'''

# Write the raw data to a file
mssparkutils.fs.put(inputPath + "data.txt", device_data, True)
print("✅ Source stream created.")

# Read the file as plain text (line-by-line)
raw_df = spark.read.text(inputPath + "data.txt")

# Define safe JSON parser that handles bad lines
def safe_parse(row):
    line = row.value  # In Fabric notebooks, use `.value` instead of `.text`
    try:
        parsed = json.loads(line)
        return Row(device=parsed.get("device"), status=parsed.get("status"), raw=None)
    except Exception:
        return Row(device=None, status=None, raw=line)

# Apply the parser to each row
parsed_rdd = raw_df.rdd.map(safe_parse)
parsed_df = spark.createDataFrame(parsed_rdd)

# Separate valid and invalid rows
valid_data_df = parsed_df.filter("device IS NOT NULL AND status IS NOT NULL")
invalid_data_df = parsed_df.filter("device IS NULL AND status IS NULL")

# Show valid data
print("✅ Valid rows:")
valid_data_df.show(truncate=False)

# Show invalid data
print("⚠️ Invalid rows (malformed JSON):")
invalid_data_df.show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.text("Files/eventdata/data.txt")
# df now is a Spark DataFrame containing text data from "Files/eventdata/data.txt".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define Lakehouse output folders (under Files or Tables)
valid_output_path = "Files/eventdata_output/valid/"
invalid_output_path = "Files/eventdata_output/invalid/"

# Write valid rows as JSON
valid_data_df.write.mode("overwrite").json(valid_output_path)
print(f"✅ Valid data written to: {valid_output_path}")

# Write invalid rows as JSON (or CSV if easier to inspect)
invalid_data_df.write.mode("overwrite").json(invalid_output_path)
print(f"⚠️ Invalid data written to: {invalid_output_path}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.option("multiline", "true").json("Files/eventdata_output/valid/part-00000-5604908a-af6e-45fd-ab7a-81ac7e243750-c000.json")
# df now is a Spark DataFrame containing JSON data from "Files/eventdata_output/valid/part-00000-5604908a-af6e-45fd-ab7a-81ac7e243750-c000.json".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.option("multiline", "true").json("Files/eventdata_output/invalid/part-00000-e67a86b4-e5b1-4579-918e-372740e2603f-c000.json")
# df now is a Spark DataFrame containing JSON data from "Files/eventdata_output/invalid/part-00000-e67a86b4-e5b1-4579-918e-372740e2603f-c000.json".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Optional: Show counts for clarity
print(f"Valid rows count: {valid_data_df.count()}")
print(f"Invalid rows count: {invalid_data_df.count()}")

# Repartition for better file output (adjust number as needed)
valid_data_df = valid_data_df.repartition(1)
invalid_data_df = invalid_data_df.repartition(1)

# Define Lakehouse paths
valid_output_path = "Files/eventdata_output/valid/"
invalid_output_path = "Files/eventdata_output/invalid/"

# Write valid rows to JSON (overwrite old files)
valid_data_df.write.mode("overwrite").json(valid_output_path)
print(f"✅ Valid rows written to: {valid_output_path}")

# Write invalid rows to JSON
invalid_data_df.write.mode("overwrite").json(invalid_output_path)
print(f"⚠️ Invalid rows written to: {invalid_output_path}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
