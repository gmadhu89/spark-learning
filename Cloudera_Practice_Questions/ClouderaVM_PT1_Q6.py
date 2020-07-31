'''

use gmadhu89_retail_db;
create table products
(
product_id int,
product_category_id int,
product_name string,
prod_description string,
product_price float,
product_image string
)
row format delimited fields terminated by ','
stored as textfile;

load data local inpath "/data/retail_db/products/part-00000" into table products;


Get products from metastore table named "product_replica" whose price > 100 and save the results in HDFS in parquet format

Result should be saved in /user/cloudera/practice1/problem8/product/output as parquet file
Files should be saved in uncompressed format.

'''

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Table to DF").getOrCreate()
sc = spark.sparkContext

table_import = "select * from gmadhu89_retail_db.products"

products = spark.sql(table_import)

product_filter = products.filter(products.product_price > 100).write.mode("overwrite").option("compression","uncompressed").format("parquet").save("/user/gmadhu89/q6/output")