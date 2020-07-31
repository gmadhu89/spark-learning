orders=spark.read.csv("/public/retail_db/orders/part-00000",sep=',',schema='order_id int,order_date string,order_customer_id int,order_status string')
from pyspark.sql.types import IntegerType,FloatType


orderItemsDF=spark.read.csv("/public/retail_db/order_items/part-00000",sep=',').toDF('order_item_id','order_item_order_id','order_item_product_id','order_item_quantity','order_item_subtotal','order_item_product_price')
orderItems=orderItemsDF.withColumn('order_item_id',orderItemsDF.order_item_id.cast(IntegerType())) \
.withColumn('order_item_order_id',orderItemsDF.order_item_order_id.cast(IntegerType())) \
.withColumn('order_item_product_id',orderItemsDF.order_item_product_id.cast(IntegerType())) \
.withColumn('order_item_quantity',orderItemsDF.order_item_quantity.cast(IntegerType())) \
.withColumn('order_item_subtotal',orderItemsDF.order_item_subtotal.cast(FloatType())) \
.withColumn('order_item_product_price',orderItemsDF.order_item_product_price.cast(FloatType()))

from pyspark.sql.window import *
from pyspark.sql.functions import round,sum

spec = Window.partitionBy(orderItems.order_item_order_id)

orderItems.withColumn('order_revenue',round(sum(orderItems.order_item_subtotal).over(spec),2).show()

############################### GET TOP N PRODUCTS PER DAY ######################################

daily_product_revenue= orders. \
filter('order_status in ("COMPLETE","CLOSED")').\
join(orderItems,orders.order_id == orderItems.order_item_order_id).\
groupBy('order_date','order_item_product_id').\
agg(round(sum(orderItems.order_item_subtotal),2).alias('revenue'))

daily_product_revenue_sorted= daily_product_revenue.sort(daily_product_revenue.order_date,daily_product_revenue.revenue.desc())


from pyspark.sql.functions import sum,min,max,avg,round


spec = Window.partitionBy('departmentId')
employees.select('employee_id','department_id','salary'). \
		 withColumn('salary_expense',sum('salary').over(spec)). \
		 withColumn('min_salary',min('salary').over(spec)). \
		 withColumn('max_salary',max('salary').over(spec)). \
		 withColumn('avg_salary',avg('salary').over(spec)). \
		 sort('department_id'). \
		 show()



#To perform operations over derived columns.
from pyspark.sql.functions import col
#Calculating Percentage

employees.select('employee_id','department_id','salary'). \
		  withColumn('salary_expense',sum('salary').over(spec)). \
		  withColumn('percentage_salary',employees.salary/col('salary_expense')*100)


##Lead, Lag, First , Last
spec=Window.partitionBy('department_id'). \
		   .orderBy(employees.salary.desc())


from pyspark.sql.functions import lead,lag,first,last
employees.select('employee_id','department_id','salary'). \
		 .withColumn('next_salary_employee',lead('employee_id').over(spec)). \
		 sort('department_id',employees.salary.desc()). \
		 show()


##Second highest paid
employees.select('employee_id','department_id','salary'). \
		 .withColumn('next_salary_employee',lead('employee_id',2).over(spec)). \
		 sort('department_id',employees.salary.desc()). \
		 show()		 

##Similar Window function can be applied for Lag, First, Last

##For Last alone , be default you will not get the last value for the criteria given. We have to specifc unboundedPreceeding and unBoundedFollowing.
spec = Window.partitionBy('department_id').\
			 .orderBy(employees.salary.desc()). \
			 .rangeBetween(Window.unboundedPreceeding,Window.unBoundedFollowing)


employees.select('employee_id','department_id','salary'). \
		 .withColumn('last_salary',last('employees.salary',False).over(spec)). \
		 .orderBy(employees.salary.desc()). \
		 show()


##Rank, denRank and RowNumber

from pyspark.sql.functions import rank,dense_rank,row_number
spec=Window.partitionBy('department_id'). \
		   orderBy(employees.salary.desc())


employees.select('employee_id','department_id','salary'). \
		 withColumn('rank',rank.over(spec)). \
		 withColumn('dense_rank',dense_rank.over(spec)). \
		 withColumn('row_number',row_number.over(spec)). \
		 orderBy('department_id',employees.salary.desc()). \
		 show()