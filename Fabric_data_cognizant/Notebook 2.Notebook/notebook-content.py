# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a8f029ec-0391-400b-b028-11a839df473f",
# META       "default_lakehouse_name": "lakehouse_delta",
# META       "default_lakehouse_workspace_id": "8bfcedba-edeb-4a23-9465-bbad5c8a3409",
# META       "known_lakehouses": [
# META         {
# META           "id": "56dbacad-1cb3-4aca-a2e5-703f34f92a3d"
# META         },
# META         {
# META           "id": "f8ea80db-0651-4b5d-a230-50df175f61fa"
# META         },
# META         {
# META           "id": "a8f029ec-0391-400b-b028-11a839df473f"
# META         },
# META         {
# META           "id": "da36a83f-cbc2-4046-8ddd-80b7dd12cc72"
# META         },
# META         {
# META           "id": "2493a8c7-6538-46e5-942b-0b1ce696cc6f"
# META         },
# META         {
# META           "id": "437f1d02-681d-4e7d-8697-a012b735446b"
# META         },
# META         {
# META           "id": "8c4dc410-df2b-45a7-a881-972e0bd4bd08"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("abfss://dp700_cognizant@onelake.dfs.fabric.microsoft.com/lakehouse_cognizant.Lakehouse/Files/sales.csv")
# df now is a Spark DataFrame containing CSV data from "abfss://dp700_cognizant@onelake.dfs.fabric.microsoft.com/lakehouse_cognizant.Lakehouse/Files/sales.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
