sqoop import --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" --username root --password cloudera \
--direct \ #use direct option in mysql for fast execution
--table products \ 
--target-dir /user/cloudera/problem2/products1 \
--fields-terminated-by "|" \ 
--as-textfile


case class Products(
product_id :Integer,
product_category_id :Integer,
product_name :String,
product_description :String,
product_price :Float,
product_image :String
)

val data = sc.textFile("/user/cloudera/problem2/products1").map( x => {
	val rec = x.split("\\|")     // | is regex so escaping is necessary else incorrect result
Products(rec(0).toInt,rec(1).toInt,rec(2),rec(3),rec(4).toFloat,rec(5))
}).
filter( x => x.product_price < 100)

// using rdd
val statsByCategoryRDD = data.
map(x=>(x.product_category_id,x.product_price)). // value 1 = product prices
aggregateByKey((0.0,0,0.0,Float.MaxValue)) ( // highest value, total products, avg prices, min price 
	(acc,value1) => (math.max(acc._1,value1), acc._2+1, acc._3+value1, math.min(acc._4,value1) ),
	(acc1,acc2) => (math.max(acc1._1,acc2._1), acc1._2+acc2._2,acc1._3+acc2._3, math.min(acc1._4,acc2._4) )
).map(x=>(x._1, x._2._1,x._2._2,x._2._3/x._2._2,x._2._4)).
sortBy(_._1,false)

// using df
val statsByCategoryDF = data.toDF().
groupBy($"product_category_id").
agg(max(col("product_price")).alias("highest_value"), countDistinct($"product_id").alias("total_products"),
 avg(col("product_price")).alias("average_price"), min(col("product_price")).alias("min_value") ).
orderBy($"product_category_id".desc)


//using sql
data.toDF().registerTempTable("products");
val statsByCategorySQL = sqlContext.sql(""" 
select product_category_id, max(product_price) as highest_value, count(distinct(product_id)) as products,
avg(product_price) as avg_price, min(product_price) as min_value from products group by product_category_id
order by product_category_id desc
	""")

// save output 
import com.databricks.spark.avro._
sqlContext.setConf("spark.sql.avro.compression.codec","snappy")
statsByCategoryRDD.toDF().write.avro("/user/cloudera/problem2/products/result-rdd")
statsByCategoryDF.write.avro("/user/cloudera/problem2/products/result-df")
statsByCategorySQL.write.avro("/user/cloudera/problem2/products/result-sql")