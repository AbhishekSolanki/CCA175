// import as text
sqoop import --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
--username root --password cloudera \
--table orders \
--target-dir /user/cloudera/problem5/text \
--fields-terminated-by "\t" \
--lines-terminated-by "\n"

// import as avro
sqoop import --connect "jdbc:mysql://quickstart:3306/retail_db" \
--username root --password cloudera \
--table orders \
--as-avrodatafile \
--target-dir /user/cloudera/problem5/avro

//import as parquet
sqoop import --connect "jdbc:mysql://quickstart:3306/retail_db" \
--username root --password cloudera \
--table orders \
--as-parquetfile \
--target-dir /user/cloudera/problem5/parquet

//1. Read avro and store as parquet snappy, text gzip, sequence
import com.databricks.spark.avro._
val avroData = sqlContext.read.avro("/user/cloudera/problem5/avro")
sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
avroData.write.parquet("/user/cloudera/problem5/parquet-snappy-compress")

avroData.map(x=>(x(0)+"\t"+x(1)+"\t"+x(2)+"\t"+x(3))).saveAsTextFile("/user/cloudera/problem5/text-gzip-compress",
	classOf[org.apache.hadoop.io.compress.GzipCodec])

avroData.map(x=>(x(0).toString,x(0)+"\t"+x(1)+"\t"+x(2)+"\t"+x(3))).saveAsSequenceFile("/user/cloudera/problem5/sequence")

// alternate way using sqoop for text snappy. text snappy may fail in VMs
sqoop import --table orders --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
 --username retail_dba --password cloudera --as-textfile -m1 \
 --target-dir user/cloudera/problem5/text-snappy-compress \
 --compress --compression-codec org.apache.hadoop.io.compress.SnappyCodec

// read parquet and save as parquet uncompress, avro snappy
val parquetData = sqlContext.read.parquet("/user/cloudera/problem5/parquet-snappy-compress")
sqlContext.setConf("spark.sql.parquet.compression.codec","uncompressed")
parquetData.write.parquet("/user/cloudera/problem5/parquet-no-compress")

import com.databricks.spark.avro._
sqlContext.setConf("spark.sql.avro.compression.codec","snappy")
parquetData.write.avro("/user/cloudera/problem5/avro-snappy")

// read avro snappy and store as json uncompress, json gzip 
val avroSnapyData = sqlContext.read.avro("/user/cloudera/problem5/avro-snappy")
sqlContext.setConf("spark.sql.json.compression.codec","uncompress")
avroSnapyData.write.json(" /user/cloudera/problem5/json-no-compress")
sqlContext.setConf("spark.sql.json.compression.codec","gzip")
avroSnapyData.write.json("/user/cloudera/problem5/json-gzip")


// read json gzip and save as text gzip
val jsonGzipData = sqlContext.read.json("/user/cloudera/problem5/json-gzip")
jsonGzipData.map(x=>(x(0)+","+x(1)+","+x(2)+","+x(3))).saveAsTextFile("/user/cloudera/problem5/csv-gzip",
classOf[org.apache.hadoop.io.compress.GzipCodec])

// read sequence and store as orc
// to get the type of kv of sequence file
hadoop fs -cat  /user/cloudera/problem5/sequence/part-00000 | cut -c-300

val sequenceData = sc.sequenceFile("/user/cloudera/problem5/sequence/",
classOf[org.apache.hadoop.io.Text], classOf[org.apache.hadoop.io.Text])
val seqData = sequenceData.map(x => {
var rec = x._2.toString.split("\t")
(rec(0),rec(1),rec(2),rec(3))
})
seqData.toDF().write.orc("/user/cloudera/problem5/orc")