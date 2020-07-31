'''
sqoop import --connect "jdbc:mysql://localhost/retail_db" --username root --password cloudera --table products --warehouse-dir /user/cloudera/practice5.db --hive-import --create-hive-table --hive-database default --hive-table product_ranked_new -m 1

using product_ranked_new metastore table, Find the top 5 most expensive products within each category 

gmadhu89_retail_db.products

Output Requirement:

Output should have product_category_id,product_name,product_price.

Result should be saved in /user/cloudera/pratice4/question2/output

Output should be saved as pipe delimited file.

'''

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Practice Test Question 3").getOrCreate()
sc = spark.sparkContext

products = spark.sql("select * from gmadhu89_retail_db.products")

products.createOrReplaceTempView("products")

sql_cmd = "select concat(product_category_id,'|',product_name,'|',product_price) as final \
from (select product_category_id,product_name,product_price, \
dense_rank() over (partition by product_category_id order by product_price desc) Rnk \
from products) where Rnk <=5"

products_final = spark.sql(sql_cmd)

products_final.write.format("text").save("/user/gmadhu89/PT3/Q5/Output")

'''
from pyspark.sql import Window
from pyspark.sql.functions import dense_rank,col,concat,lit

spec = Window.partitionBy(products.product_category_id).orderBy(products.product_price.desc())

products2 = products.withColumn("Rnk",dense_rank().over(spec)).filter(col('Rnk') <= 5)

final = products2.withColumn("Final",concat(col('product_category_id'),lit('|'),col('product_name'),lit('|'),col('product_price'))).select('Final')

'''