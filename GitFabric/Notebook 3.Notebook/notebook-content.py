# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8743a788-2cdd-4354-bf94-a71111594303",
# META       "default_lakehouse_name": "LakehouseOct9",
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

df = spark.sql("SELECT * FROM LakehouseOct9.aaaaa LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("DELETE FROM aaaaa WHERE quantity = 8")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# below is syntax
# df_original = spark.read.format("delta").option("versionAsOf", 1).load("/path/to/your/table")
df_original = spark.read.format("delta").option("versionAsOf", 1).load("abfss://Activator@onelake.dfs.fabric.microsoft.com/LakehouseOct9.Lakehouse/Tables/aaaaa")

# abfss://Activator@onelake.dfs.fabric.microsoft.com/LakehouseOct9.Lakehouse/Tables/aaaaa
# df_original = spark.read.format("delta").option("versionAsOf", 1).load(abfss://Activator@onelake.dfs.fabric.microsoft.com/LakehouseOct9.Lakehouse/Tables/aaaaa)
# above is wrong : THE PATH MUST BE IN DOUBLE QUOTE



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_original)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM LakehouseOct9.aaaaa LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_old = spark.read.format("delta").option("versionAsOf", 1).load("abfss://Activator@onelake.dfs.fabric.microsoft.com/LakehouseOct9.Lakehouse/Tables/aaaaa")
df_old.write.format("delta").mode("overwrite").save("abfss://Activator@onelake.dfs.fabric.microsoft.com/LakehouseOct9.Lakehouse/Tables/bbbbb")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
