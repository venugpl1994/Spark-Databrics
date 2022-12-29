# Spark-Databrics

from pyspark.sql.functions import col,lit
# File location and type
file_location = "/FileStore/tables/test_1.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)


#write into csv file 

df3.write.mode("append").csv("/FileStore/tables/test_2.csv") ////It will create new parquet formate file in DBFS 

df3.write.mode("overwrite").csv("/FileStore/tables/test_2.csv")///It will not create new paruet file in DBFS and overwrite the earlier file 

df.collect() ///It will show row wise data 

df.write.format("parquet").mode("overwrite").save("/Filestore/tables/Employment")

How to create secret scope in databrics or How to connect databrics with Azure key-vault account ?

Enter #secrets/createScope in databrics notebook and connect with Azure key-vault 

df1=df.select("city","state") ---General statement
