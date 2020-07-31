'''
Problem Statement:

Data is available in local file system /data/retail_db
Source directories: /data/retail_db/orders and /data/retail_db/customers
Source delimiter: comma (“,”)
Source Columns - orders - order_id, order_date, order_customer_id, order_status
Source Columns - customers - customer_id, customer_fname, customer_lname and many more
Get the customers who have not placed any orders, sorted by customer_lname and then customer_fname
Target Columns: customer_lname, customer_fname
Number of files - 1
Target Directory: /user/<YOUR_USER_ID>/solutions/solutions02/inactive_customers
Target File Format: TEXT
Target Delimiter: comma (“, ”)
Compression: N/A

'''

'''
Execution Command
spark-submit --master yarn \
--conf spark.ui.port=12709 \
--executor-cores 2 \
--num-executors 6 \
--executor-memory 3G \
Get_Inactive_Customers_RDD.py
'''

from pyspark import SparkContext,SparkConf
conf=SparkConf().setAppName('Inactive customers RDD solution')
sc=SparkContext(conf=conf)

ordersFile=open("/data/retail_db/orders/part-00000").read().splitlines()
customersFile=open("/data/retail_db/customers/part-00000").read().splitlines()

orders=sc.parallelize(ordersFile)
customers=sc.parallelize(customersFile)

ordersMap=orders.map(lambda o:( int(o.split(",")[2]), o ) )
customersMap=customers.map(lambda c: ( int(c.split(",")[0]), c.split(",")[1] + "," + c.split(",")[2]) )

CustomerOrderJoin = customersMap.leftOuterJoin(ordersMap)
inactiveCustomers = CustomerOrderJoin.filter(lambda c:c[1][1]==None)

inactiveFinal = inactiveCustomers.map(lambda ic: ( (ic[1][0].split(",")[1],ic[1][0].split(",")[0]) , ic[1][0].split(",")[1] + ", " + ic[1][0].split(",")[0]))
inactiveFinalSorted = inactiveFinal.sortByKey()
FinalResult=inactiveFinalSorted.map(lambda x: x[1])

path="/user/gmadhu89/solutions/solutions02/inactive_customers"
import subprocess
if subprocess.call(["hdfs","dfs","-test","-e",path]) == 0:
	subprocess.call(["hadoop","fs","-rm","-R",path])

FinalResult.coalesce(1).saveAsTextFile("/user/gmadhu89/solutions/solutions02/inactive_customers")