// 1. sqoop import
sqoop import --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
--username root \
--password cloudera \
--table orders \
--target-dir  /user/cloudera/problem1/orders \
--compress --compression-codec org.apache.hadoop.io.compress.SnappyCodec \
--as-avrodatafile

//2 sqoop import 
sqoop import --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
--username root --password cloudera --table order_items \
--target-dir /user/cloudera/problem1/order-items \
--compress --compression-codec org.apache.hadoop.io.compress.SnappyCodec \
--as-avrodatafile

// 3 joining dataframes
import com.databricks.spark.avro._
val orderRawDF = sqlContext.read.avro("/user/cloudera/problem1/orders")
val orderItemsRawDF = sqlContext.read.avro("/user/cloudera/problem1/order-items")

val joinedOrderDF = orderRawDF.join(orderItemsRawDF,orderRawDF("order_id")===orderItemsRawDF("order_item_order_id"))

//4. aggregation, sorting and ordering
// using df
joinedOrderDF.groupBy(to_date(from_unixtime(col("order_date")/1000)).alias("order_date_format"),col("order_status")).
agg(round(sum("order_item_subtotal"),2).alias("total_amount"),
countDistinct("order_id").alias("total_orders")).
orderBy(col("order_date_format").desc,col("order_status"),col("total_amount").desc,col("total_orders")).
show()


root
 |-- order_id: integer (nullable = true)
 |-- order_date: long (nullable = true)
 |-- order_customer_id: integer (nullable = true)
 |-- order_status: string (nullable = true)
 |-- order_item_id: integer (nullable = true)
 |-- order_item_order_id: integer (nullable = true)
 |-- order_item_product_id: integer (nullable = true)
 |-- order_item_quantity: integer (nullable = true)
 |-- order_item_subtotal: float (nullable = true)
 |-- order_item_product_price: float (nullable = true)


//using rdd


val combinerInit = (x:(Float,String)) => (x._1,Set(x._2))
val merger1 = (x:(Float,Set[String]),y:(Float,String))=>(x._1+y._1,x._2+y._2)
val combiner =(x:(Float,Set[String]),y:(Float,Set[String]))=>(x._1+y._1,x._2++y._2)

val result = joinedOrderDF.
map( x=> ((x(1).toString,x(3).toString),(x(8).toString.toFloat,x(0).toString)) ).  //key is (date,status) value is (subtotal,orderid)
combineByKey(combinerInit,merger1,combiner).
map(x => (x._1._1,x._1._2,x._2._1,x._2._2.size)).
toDF().orderBy(col("_1").desc,col("_2"),col("_3"),col("_4").desc)

//same sorting using rdd
sortBy(x => (x._2,x._3,-x._4)).sortBy(x=>x._1,false)

//5. writing to parquet with compression
sqlContext.setConf("spark.sql.parquet.ompression.codec","snappy")
result.write.parquet("/user/cloudera/problem1/result-snappy")


//6. exporting back to mysql
result.map(x=>(x(0)+","+x(1)+","+x(2)+","+(3)).saveAsTextFile("/user/cloudera/problem1/result-csv")

create table retail_db.result(order_date varchar(255) not null,order_status varchar(255) not null, total_orders int, total_amount numeric, constraint pk_order_result primary key (order_date,order_status)); 

sqoop export --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" --username root --password cloudera \
--table result --export-dir "/user/cloudera/problem1/result-csv" \
--columns "order_date,order_status,total_amount,total_orders"