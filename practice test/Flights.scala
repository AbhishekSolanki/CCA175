// flight data analytics using spark
//(Flight_number,origin,dest,distance,price)
val flights = sc.parallelize(Array(("6E101","BLR","AMD",1600,3486.5),("6E101","AMD","DEL",900,2280.33),("6E101","DEL","IXJ",500,5600.10),
	("9W662","DEL","BOM",1200,3489.23),("9W662","BOM","PNQ",200,1765.1),("9W662","DEL","PNQ",1400,4876.12),("SG123","BOM","DEL",1200,3398.00),
	("SG234","BLR","BOM",100,2300.0),("9W098","AMD","PNQ",800,3291.11),("9W098","PNQ","BLR",800,1997.0),("9W669","DEL","MAA",2500,5298.11),
	("AI003","BOM","CCU",1500,4532.77),("GA876","DEL","CCU",2000,4322.54),("GA567","BOM","MAA",2000,3673.22),
	("6E508","AMD","BLR",1600,3777.77),("6E508","BOM","DEL",1600,3774.77)))
//flights: org.apache.spark.rdd.RDD[(String, String, String, Int,Double)] = ParallelCollectionRDD[0] at parallelize at <console>:27
/*(6E101,BLR,AMD,1600,3486.5)
(6E101,AMD,DEL,900,2280.33)
(6E101,DEL,IXJ,500,5600.1)
(9W662,DEL,BOM,1200,3489.23)
(9W662,BOM,PNQ,200,1765.1)
(9W662,DEL,PNQ,1400,4876.12)
(SG123,BOM,DEL,1200,3398.0)
(SG234,BLR,BOM,100,2300.0)
(9W098,AMD,PNQ,800,3291.11)
(9W098,PNQ,BLR,800,1997.0)
(9W669,DEL,MAA,2500,5298.11)
(AI003,BOM,CCU,1500,4532.77)
(GA876,DEL,CCU,2000,4322.54)
(GA567,BOM,MAA,2000,3673.22)
(6E508,AMD,BLR,1600,3777.77)
*/

//1. Find the total and  average distance of airlines for a day
val flightTotalAndAvgStats = flights.map( x=> (x._1.substring(0,2),x._4.toDouble)).
 aggregateByKey((0.0,0.0))(
( (acc:(Double,Double),value:Double) => ( acc._1+value, acc._2 + 1 )),
( (acc1:(Double,Double),acc2:(Double,Double)) => ( acc1._1 + acc2._1, acc1._2 + acc2._2) )
 ).map( x=> (x._1,x._2._1,x._2._1/x._2._2))
 flightTotalAndAvgStats.take(20).foreach(println)
/* (9W,6900.0,1150.0)
(SG,1300.0,650.0)
(GA,4000.0,2000.0)
(AI,1500.0,1500.0)
(6E,4600.0,1150.0)*/


//2. Find airport having highest destination and average distance from origin to destination
import scala.collection.mutable.HashSet
val airportWithMaxDestination = flights.map(x => (x._2,x)).
 combineByKey(
( (x:(String, String, String, Int)) => (HashSet[String](x._3),x._4.toDouble) ), // PTR: DO NOT specify combiner output as empty/null/0 1 record would be misses
( (acc:(HashSet[String],Double),value:(String, String, String, Int)) => (acc._1 += value._3,acc._2+value._4 ) ),
( (acc1:(HashSet[String],Double), acc2:(HashSet[String],Double)) => (acc1._1++acc2._1,acc1._2+acc2._2))
 ).map( x => (x._1,x._2._1.size,x._2._2/x._2._1.size))

 airportWithMaxDestination.take(20).foreach(println)
/*(BOM,4,1225.0)
(BLR,2,850.0)
(AMD,3,1100.0)
(DEL,5,1520.0)
(PNQ,1,800.0)*/

//3. Find the highest priced flight for each airline company and sort the result based on prices from High to Low
case class FlightRank(
airline :String,
flight_number :String,
distance :Double,
price :Double
)

import org.apache.spark.sql.expressions.Window
val airlineBucket = Window.partitionBy("airline").orderBy($"distance".desc)

val flightRankByAirline = flights.map( x=> (x._1,(x._4,x._5))). 
aggregateByKey((0.0,0.0))( // initial value (total_distance, total_flight)
( (acc,value) => (acc._1 + value._1, acc._2 + value._2) ),
( (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2) )
).map(x=> FlightRank(x._1.substring(0,2),x._1,x._2._1,x._2._2)). // (airline_code,x)
toDF().
withColumn("rank", dense_rank over airlineBucket).
where(col("rank")===1).
orderBy($"price".desc)
/*+-------+-------------+--------+--------+----+                                  
|airline|flight_number|distance|   price|rank|
+-------+-------------+--------+--------+----+
|     6E|        6E101|  3000.0|11366.93|   1|
|     9W|        9W662|  2800.0|10130.45|   1|
|     AI|        AI003|  1500.0| 4532.77|   1|
|     GA|        GA876|  2000.0| 4322.54|   1|
|     GA|        GA567|  2000.0| 3673.22|   1|
|     SG|        SG123|  1200.0|  3398.0|   1|
+-------+-------------+--------+--------+----+
*/

//4. Search the top 3 flight based on orig and dest and select the flight with minumum price
import scala.collection.mutable.ListBuffer
val searchTopFlights = flights.map(x => ((x._2,x._3),(x._1,x._5))).
combineByKey(
( (x) => List[(String,Double,Int)]((x._1,x._2,1) )),
( (acc:List[(String,Double,Int)],value) => (value._1,value._2,0)::acc ),
( (acc1:List[(String,Double,Int)],acc2:List[(String,Double,Int)]) => {
	var aa = acc1 ::: acc2
	var retList = ListBuffer[(String,Double,Int)]()
	aa.sortBy(_._2).zipWithIndex.foreach {
		case(el,i) => retList+= ((el._1,el._2,i+1)) 
	}
	retList.toList
})
)

//5. apply filter on problem 3 having distance > 1500
val flightDistGt1500 = flightRankByAirline.filter(flightRankByAirline.price>1500) 
//6. store the result in metastore table 
