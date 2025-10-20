# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8743a788-2cdd-4354-bf94-a71111594303",
# META       "default_lakehouse_name": "Lakehouse",
# META       "default_lakehouse_workspace_id": "d3882e81-ca32-4c69-8e0a-8b00346c2f4b",
# META       "known_lakehouses": [
# META         {
# META           "id": "8743a788-2cdd-4354-bf94-a71111594303"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/sales.csv")
# df now is a Spark DataFrame containing CSV data from "Files/sales.csv".
display(df)

df.write.format("delta").saveAsTable("myexternaltable", path="Files/myexternaltable")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
