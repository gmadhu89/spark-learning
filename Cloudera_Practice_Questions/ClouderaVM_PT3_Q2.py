'''
sqoop import --connect "jdbc:mysql://localhost/retail_db" --password cloudera --username root --table customers --target-dir /user/cloudera/problem3/customer/permissions

Change permissions of all the files under /user/cloudera/problem3/customer/permissions such that 
owner has read,write and execute permissions, group has read and write permissions whereas others have just read and execute permissions


customers.coalesce(4).write.format("csv").save("/user/gmadhu89/PT3/Q2/Files")
'''


hadoop fs -chmod -R 765 /user/gmadhu89/PT3/Q2/Files/*