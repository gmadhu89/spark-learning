'''

sqoop import --connect "jdbc:mysql://localhost/retail_db" --username root --password cloudera --table products --warehouse-dir /user/cloudera/practice4.db --hive-import --create-hive-table --hive-database default --hive-table product_ranked -m 1
Provided  a meta-store table named product_ranked consisting of product details ,find the most expensive product in each category

Output should have product_category_id ,product_name,product_price,rank.
Result should be saved in /user/cloudera/pratice4/output/  as pipe delimited text file

'''
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Practice test 2 Question 3").getOrCreate()
sc = spark.sparkContext

products = spark.sql("select * from gmadhu89_retail_db.products")

products.createOrReplaceTempView("products")

sql_cmd = "select concat(product_category_id,'|',product_name,'|',product_price,'|',Rnk) as final \
from (select product_category_id,product_name,product_price,dense_rank() over(partition by product_category_id order by product_price desc) as Rnk \
from products) where Rnk = 1 \
"

products_ranked = spark.sql(sql_cmd)

products_ranked.write.format("text").save("/user/gmadhu89/PT2/q4/output")