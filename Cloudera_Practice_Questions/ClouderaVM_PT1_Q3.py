'''

Run below sqoop command to setup data on local cloudera vm
sqoop import --connect "jdbc:mysql://localhost/retail_db" --password cloudera --username root --table products --target-dir /user/cloudera/practice1/problem5 --as-parquetfile --split-by "product_id"

From provided parquet files located at hdfs location :
/user/cloudera/practice1/problem5
Get maximum product_price in each product_category order the results by maximum price descending.

Final output should be saved in below hdfs location:
/user/cloudera/practice1/problem5/output

Final output should have product_category_id and max_price separated by pipe delimiter
Ouput should be saved in text format with Gzip compression
Output should be stored in a single file.

'''
from pyspark.sql import SparkSession
spark=SparSession.builder.appName("Maximum product price").getOrCreate()
sc = spark.sparkContext

from pyspark.sql.functions import col
from pyspark.sql.functions import lit

products = spark.read.csv("/public/retail_db/products/part-00000",sep=",",schema="product_id int,product_category_id int,product_name string,\
product_description string,product_price float,product_image string")

products.createOrReplaceTempView("products")

sql_cmd = "select concat(product_category_id,'|',max(product_price)) as final from products \
group by product_category_id \
order by max(product_price) desc"

products_final = spark.sql(sql_cmd)

products_final.coalesce(1).write.mode("overwrite").format("text").option("compression","gzip").save("/user/gmadhu89/practice1/problem5/output1")


'''
sql_cmd = "select product_category_id,max(product_price) as max_price from products \
group by product_category_id \
order by max(product_price) desc"

products_final_test = spark.sql(sql_cmd)
from pyspark.sql.functions import concat
from pyspark.sql.types import IntegerType,StringType
products_delim = products_final_test.withColumn("category_Str",products_final_test.product_category_id.cast(StringType()))\
.withColumn("price_Str",products_final_test.max_price.cast(StringType()))


products_store = products_delim.withColumn("Final",concat(col('category_Str'),lit('|'),col('price_Str'))) \
.drop('product_category_id').drop('max_price').drop('category_Str').drop('price_Str')

crimeDataFormatted = crimeData.select('Date','Primary Type'). \
withColumn('Date_New', concat(substring(crimeData.Date,7,4),substring(crimeData.Date,1,2)).cast(IntegerType()))
'''


#products_final.coalesce(1).write.csv("/user/gmadhu89/practice1/problem5/output",sep="|",compression='gzip')