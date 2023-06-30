# Databricks notebook source
# This folder is for you to write any data as needed. Write access is restricted elsewhere. You can always read from dbfs.
aws_role_id = "AROAUQVMTFU2DCVUR57M2"
user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
userhome = f"s3a://e2-interview-user-data/home/{aws_role_id}:{user}"
print(userhome)

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import java.net.URL
# MAGIC import java.io.File
# MAGIC import org.apache.commons.io.FileUtils
# MAGIC
# MAGIC val tmpFile2 = new File("/tmp/listings.csv.gz")
# MAGIC FileUtils.copyURLToFile(new URL("http://data.insideairbnb.com/united-states/oh/columbus/2023-03-28/data/listings.csv.gz"), tmpFile2)

# COMMAND ----------

# https://docs.python.org/3/library/hashlib.html#blake2
from hashlib import blake2b

user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
h = blake2b(digest_size=4)
h.update(user.encode("utf-8"))
display_name = "user_" + h.hexdigest()
print("Display Name: " + display_name)

dbutils.fs.cp('file:/tmp/listings.csv.gz', userhome + '/listings.csv.gz')
dbutils.fs.cp(userhome + '/listings.csv.gz' ,f"dbfs:/tmp/{display_name}/listings.csv.gz")
path = f"dbfs:/tmp/{display_name}/listings.csv.gz"

print("Airbnb Listings : " + path)
dbutils.fs.head(path)


# COMMAND ----------

from pyspark.sql import SparkSession
import pandas as pd

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()
airbnb_path = "dbfs:/tmp/user_71b00bb9/listings.csv.gz"

# Read the CSV data into a PySpark DataFrame
df_spark = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(airbnb_path)

# Convert PySpark DataFrame to Pandas DataFrame
df_pandas = df_spark.toPandas()

# for file in glob.glob(airbnb_path):
#     df = pd.read_csv(file)
    
#     # Convert dataframe to sql table                                   
#     df.to_sql(file[:-4], spark, index=False)
#     # table_name = os.path.splitext(os.path.basename(file))[0]
#     df_spark.createOrReplaceTempView(table_name)
#     print('done')


# Create temporary table
df_spark.createOrReplaceTempView("airbnb")

# Query the temporary table
spark.sql("SELECT * FROM airbnb LIMIT 20").show()


# COMMAND ----------

# MAGIC %sql
# MAGIC USE airbnb; 
# MAGIC SELECT id, listing_url, name, 30 - availability_30 AS booked_out_30 , 
# MAGIC CAST(REPLACE(Price,'$','') AS UNSIGNED) AS price_clean, 
# MAGIC CAST(REPLACE(Price,'$','') AS UNSIGNED)*(30 - availability_30) / beds AS proj_rev_30
# MAGIC FROM listings ORDER BY proj_rev_30 DESC LIMIT 20; 
