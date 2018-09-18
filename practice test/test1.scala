//PROBLEM 1 : SQOOP IMPORT
sqoop import --connect "jdbc:mysql://quickstart.cloudera/retail_db" \
--username root \
--password cloudera \
--table customers \
--compress -compression-codec snappy \
--target-dir /user/cloudera/problem1/customers/avrodata \
--fields-terminated-by '|' \
--where "customer_state='CA'" \
--as-avrodatafile;


//PROBLEM 2: SQOOP EXPORT
sqoop export --connect "jdbc:mysql://quickstart.cloudera/retail_db" \
--username root --password cloudera \
--table customer_new \
--export-dir /user/cloudera/problem1/customers/text2
--input-fields-terminated-by '^';


//PROBLEM 3 AVRO to PARQUETS
val dataFile = sqlContext.read.format("com.databricks.spark.avro").
load("/user/cloudera/problem2/avro")
sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
dataFile.write.parquet("/user/cloudera/problem2/parquet-snappy")


//PROBLEM 4 JOINS and filters
// USING DAFATRAME
case class orders(customer_id:Integer, order_id:Integer, order_status:String)
val ordersDF=sc.textFile("/user/cloudera/practice4/question3/orders/").map(x=>x.split(",")).map(c=>orders(c(2).toInt,c(0).toInt,c(3))).toDF()

case class customer(customer_id: String,customer_fname: String)
val customerDF=sc.textFile("/user/cloudera/practice4/question3/customers/").map(x=>x.split(",")).map(c=>customer(c(0),c(1))).toDF()

val result4 = customerDF.join(ordersDF,"customer_id").filter("status like '%PENDING%'")
result4.count

// USING RDDs
val ordersRDD=sc.textFile("/user/cloudera/practice4/question3/orders/").map(x=>x.split(",")).map(c=>((2).toInt,(c(0).toInt,c(3))))
val customerRDD=sc.textFile("/user/cloudera/practice4/question3/customers/").map(x=>x.split(",")).map(c=>(c(0).toInt,(c(1))))
val joinRes = customerRDD.join(ordersRDD)
val finalRes = joinRes.filter( x => x._2._2._2.contains("PENDING")).map(x=>(x._1,x._2._1,x._2._2._1,x._2._2._2))
finalRes.count


//PROBLEM 5 read parquet, compute and store as flat file
val customerRDD = sqlContext.read.parquet("/user/cloudera/problem3/customer/parquet");
customerRDD.registerTempTable("customer");
val queryRes = sqlContext.sql("""select customer_city,customer_fname,count(*) from customer 
group by customer_city,customer_fname
having customer_fname like '%Mary%' 
order by customer_fname
	""")
queryRes.rdd.map(x=>x(0)+"|"+x(1)+"|"+x(2)).saveAsTextFile("/user/cloudera/problem3/customer_grouped")


//PROBLEM6 store output as json 
case class customer(customer_id:String, customer_name:String, customer_city: String)
val customerRD = sc.textFile("/user/cloudera/problem3/customer/text").
map(x=>x.split("\t")).map(c=>customer(c(0),c(1),c(2))).toDF()
val filteredData = customerRD.filter("customer_city='Brownsville'")
filteredData.toJSON.saveAsTextFile("/user/cloudera/problem3/customer_Brownsville")


//PROBLEM7 read avro and save aas tab delimited gzip file
val customerRDD = sqlContext.read.format("com.databricks.spark.avro").
 load("/user/cloudera/problem2/customer/avro")

customerRDD.map(x=>x(0)+"\t"+x(1)+"\t"+(2)).
 saveAsTextFile("/user/cloudera/problem2/customer_text_gzip1",
 classOf[org.apache.hadoop.io.compress.GzipCodec])


 // PROBLEM 8 read from hive table
 //hive-site.xml should be available in spark conf 
 sudo cp /etc/hive/conf/hive-site.xml /etc/spark/conf/

 val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
 val resProblem8 = hiveContext.sql("select * from default.product_replica where product_price > 100")
 resProblem8.write.parquet("/user/cloudera/problem3/product/output")
 