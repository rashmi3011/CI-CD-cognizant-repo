# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "56dbacad-1cb3-4aca-a2e5-703f34f92a3d",
# META       "default_lakehouse_name": "lakehouse_Apache_spark",
# META       "default_lakehouse_workspace_id": "8bfcedba-edeb-4a23-9465-bbad5c8a3409",
# META       "known_lakehouses": [
# META         {
# META           "id": "56dbacad-1cb3-4aca-a2e5-703f34f92a3d"
# META         },
# META         {
# META           "id": "2493a8c7-6538-46e5-942b-0b1ce696cc6f"
# META         },
# META         {
# META           "id": "8c4dc410-df2b-45a7-a881-972e0bd4bd08"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("abfss://madellion_practice@onelake.dfs.fabric.microsoft.com/lakehouse_madellion.Lakehouse/Files/bronze/2019.csv")
# df now is a Spark DataFrame containing CSV data from "abfss://madellion_practice@onelake.dfs.fabric.microsoft.com/lakehouse_madellion.Lakehouse/Files/bronze/2019.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
