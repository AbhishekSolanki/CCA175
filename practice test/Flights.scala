// flight data analytics using spark

val flights = sc.parallelize(Array(("6E101","BLR","AMD",1600),("6E101","AMD","DEL",900),("6E101","DEL","IXJ",500),("9W662","DEL","BOM",1200),("9W662","BOM",
"PNQ",200),("9W662","DEL","PNQ",1400),("SG123","BOM","DEL",1200),("SG234","BLR","BOM",100),("9W098","AMD","PNQ",800),
("9W098","PNQ","BLR",800),("9W669","DEL","MAA",2500),("AI003","BOM","CCU",1500),("GA876","DEL","CCU",2000),
("GA567","BOM","MAA",2000),("6E508","AMD","BLR",1600)))
//flights: org.apache.spark.rdd.RDD[(String, String, String, Int)] = ParallelCollectionRDD[0] at parallelize at <console>:27
/*(6E101,BLR,AMD,1600)
(6E101,AMD,DEL,900)
(6E101,DEL,IXJ,500)
(9W662,DEL,BOM,1200)
(9W662,BOM,PNQ,200)
(9W662,DEL,PNQ,1400)
(SG123,BOM,DEL,1200)
(SG234,BLR,BOM,100)
(9W098,AMD,PNQ,800)
(9W098,PNQ,BLR,800)
(9W669,DEL,MAA,2500)
(AI003,BOM,CCU,1500)
(GA876,DEL,CCU,2000)
(GA567,BOM,MAA,2000)
(6E508,AMD,BLR,1600)*/

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
