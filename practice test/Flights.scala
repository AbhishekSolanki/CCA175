// flight data analytics using spark
//(Flight_number,origin,dest,distance,price)
val flights = sc.parallelize(Array(("6E101","BLR","AMD",1600,3486.5),("6E101","AMD","DEL",900,2280.33),("6E101","DEL","IXJ",500,5600.10),
	("9W662","DEL","BOM",1200,3489.23),("9W662","BOM","PNQ",200,1765.1),("9W662","DEL","PNQ",1400,4876.12),("SG123","BOM","DEL",1200,3398.00),
	("SG234","BLR","BOM",100,2300.0),("9W098","AMD","PNQ",800,3291.11),("9W098","PNQ","BLR",800,1997.0),("9W669","DEL","MAA",2500,5298.11),
	("AI003","BOM","CCU",1500,4532.77),("GA876","DEL","CCU",2000,4322.54),("GA567","BOM","MAA",2000,3673.22),
	("6E508","AMD","BLR",1600,3777.77),("6E508","BOM","DEL",1600,3774.77),("9W099","AMD","PNQ",800,2122.68),
    ("9W278","AMD","PNQ",800,1500.34)))
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
(6E508,BOM,DEL,1600,3774.77)
(9W099,AMD,PNQ,800,2122.68)
(9W278,AMD,PNQ,800,1500.34)

*/

//1. Find the total and  average distance of airlines for a day
val flightTotalAndAvgStats = flights.map( x=> (x._1.substring(0,2),x._4.toDouble)).
 aggregateByKey((0.0,0.0))(
( (acc:(Double,Double),value:Double) => ( acc._1+value, acc._2 + 1 )),
( (acc1:(Double,Double),acc2:(Double,Double)) => ( acc1._1 + acc2._1, acc1._2 + acc2._2) )
 ).map( x=> (x._1,x._2._1,x._2._1/x._2._2))
 flightTotalAndAvgStats.take(20).foreach(println)
/* (9W,8500.0,1062.5)
(SG,1300.0,650.0)
(GA,4000.0,2000.0)
(AI,1500.0,1500.0)
(6E,6200.0,1240.0)
*/


//2. Find airport having highest destination and average distance from origin to destination
// PTR: DO NOT specify combiner output as empty/null/0 1 record would be misses
import scala.collection.mutable.HashSet
val airportWithMaxDestination = flights.map(x => (x._2,x)).
 combineByKey(
( (x:(String, String, String,Int,Double)) => (HashSet[String](x._3),x._4.toDouble) ), 
( (acc:(HashSet[String],Double),value:(String, String, String, Int,Double)) => (acc._1 += value._3,acc._2+value._4 ) ),
( (acc1:(HashSet[String],Double), acc2:(HashSet[String],Double)) => (acc1._1++acc2._1,acc1._2+acc2._2))
 ).map( x => (x._1,x._2._1.size,x._2._2/x._2._1.size))

 airportWithMaxDestination.take(20).foreach(println)
/*(BOM,4,1625.0)                                                                  
(BLR,2,850.0)
(AMD,3,1633.3333333333333)
(DEL,5,1520.0)
(PNQ,1,800.0)
*/

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
|     9W|        9W662|  2800.0|10130.45|   1|
|     6E|        6E508|  3200.0| 7552.54|   1|
|     AI|        AI003|  1500.0| 4532.77|   1|
|     GA|        GA876|  2000.0| 4322.54|   1|
|     GA|        GA567|  2000.0| 3673.22|   1|
|     SG|        SG123|  1200.0|  3398.0|   1|
+-------+-------------+--------+--------+----+
*/

//4. Search the top 2 flight based on orig and dest and select the flight with minumum price
// List is immutable : All the operations creates new list
import scala.collection.mutable.ListBuffer
val searchTopFlights = flights.map(x => ((x._2,x._3),(x._1,x._5))).
combineByKey(
( (x) => List[(String,Double,Int)]((x._1,x._2,1) )), // (create combiner )executes within each partion when new key is encountered
( (acc:List[(String,Double,Int)],value) => (value._1,value._2,0)::acc ), // (merger) Execute when key already exist and append the values
( (acc1:List[(String,Double,Int)],acc2:List[(String,Double,Int)]) => { // (combiner merger) merges combiners between different partitions
	var aa = acc1 ::: acc2 // merging two list and get the new list as output
	var retList = ListBuffer[(String,Double,Int)]() //ListBuffer as we are modifying the values in existing list
	aa.sortBy(_._2).zipWithIndex.foreach { 
		case(el,i) => retList+= ((el._1,el._2,i+1)) 
	}
	retList.toList //return type is same as combiner return type which is list in this case
})
).flatMap ( x => {
 var retList = ListBuffer[(String,String,String,Double,Int)]()
 val ret = x._2.foreach {
  el => retList+= ((x._1._1,x._1._2,el._1,el._2,el._3))
 }
 retList
})

/* Without flatmap
((BOM,PNQ),List((9W662,1765.1,1)))
((DEL,CCU),List((GA876,4322.54,1)))
((AMD,PNQ),List((9W098,3291.11,1)))
((BOM,MAA),List((GA567,3673.22,1)))
((BOM,DEL),List((SG123,3398.0,1), (6E508,3774.77,2)))
((BOM,CCU),List((AI003,4532.77,1)))
((PNQ,BLR),List((9W098,1997.0,1)))
((AMD,DEL),List((6E101,2280.33,1)))
((DEL,IXJ),List((6E101,5600.1,1)))
((BLR,AMD),List((6E101,3486.5,1)))
((DEL,MAA),List((9W669,5298.11,1)))
((AMD,BLR),List((6E508,3777.77,1)))
((DEL,PNQ),List((9W662,4876.12,1)))
((BLR,BOM),List((SG234,2300.0,1)))
((DEL,BOM),List((9W662,3489.23,1)))*/

/* with flatmap applied
(BOM,PNQ,9W662,1765.1,1)
(DEL,CCU,GA876,4322.54,1)
(AMD,PNQ,9W098,3291.11,1)
(BOM,MAA,GA567,3673.22,1)
(BOM,DEL,SG123,3398.0,1)
(BOM,DEL,6E508,3774.77,2)
(BOM,CCU,AI003,4532.77,1)
(PNQ,BLR,9W098,1997.0,1)
(AMD,DEL,6E101,2280.33,1)
(DEL,IXJ,6E101,5600.1,1)*/


//5. apply filter on problem 3 having distance > 1500
val flightDistGt1500 = flightRankByAirline.filter(col("distance")>1500) 
/*+-------+-------------+--------+--------+----+                                  
|airline|flight_number|distance|   price|rank|
+-------+-------------+--------+--------+----+
|     9W|        9W662|  2800.0|10130.45|   1|
|     6E|        6E508|  3200.0| 7552.54|   1|
|     GA|        GA876|  2000.0| 4322.54|   1|
|     GA|        GA567|  2000.0| 3673.22|   1|
+-------+-------------+--------+--------+----+
*/


//6. store searchTopFlight result in metastore table with following table structure
// table structure ( orig String, dest String, flight_no String, price Double, flight_rank Int)
