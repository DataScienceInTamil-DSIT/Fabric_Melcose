# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "fc5861d0-dafc-40b3-b24d-634986b3ce9a",
# META       "default_lakehouse_name": "FridayLH",
# META       "default_lakehouse_workspace_id": "7b147420-6368-403a-bd18-d892d5687a1e",
# META       "known_lakehouses": [
# META         {
# META           "id": "fc5861d0-dafc-40b3-b24d-634986b3ce9a"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
# DRIVER:# Creating SparkSession and defining data happens on the Driver.
spark = SparkSession.builder.appName("driver_executor_demo").getOrCreate()
print("Session created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data = [
    ("Alice", 34, "Sales"),
    ("Bob",   45, "Finance"),
    ("Carol", 29, "Sales"),
    ("Dave",  40, "IT")
]
# DRIVER:# This call tells the Driver to create a logical DataFrame plan.
df = spark.createDataFrame(data, ["name", "age", "dept"])
print("DF is created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# DRIVER:
# These are *transformations*; they are just building a logical plan on the Driver.
df_filtered = df.filter(col("age") > 30)          # filter transformation
df_selected = df_filtered.select("name", "dept")  # select transformation
df_filtered.show(2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_selected1 = df_filtered.select("name", "dept")  # select transformation
df_selected1.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(type(df_selected1))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(df_selected1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
