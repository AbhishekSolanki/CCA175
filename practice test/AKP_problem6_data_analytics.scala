// create database problem6 in hive
hive -e "create database problem6"

// import all tables from mysql retail_db to wharehouse problem6 in text file format.
sqoop import-all-tables --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
--username root --password cloudera \
--hive-import \
--hive-database problem6 \
--create-hive-table \
--warehouse-dir /user/hive/warehouse/problem6.db \
--as-textfile \
-m 1


//Rank products within department by price and order by department ascending and rank descending
val productRankInDept = sqlContext.sql("""
select  p.product_id, p.product_name,p.product_price,d.department_id, 
rank() over( partition by d.department_id order by p.product_price) as product_price_rank
 from 
problem6.products p 
inner join problem6.categories c on ( p.product_category_id =category_id )
inner join problem6.departments d on (c.category_department_id = department_id)
order by d.department_id, product_price_rank desc
""");
/*[66,SOLE F85 Treadmill,2,168]                                                   
[60,SOLE E25 Elliptical,2,167]
[74,Goaliath 54" In-Ground Basketball Hoop with P,2,166]
[117,YETI Tundra 65 Chest Cooler,2,164]
[162,YETI Tundra 65 Chest Cooler,2,164]
[71,Diamondback Adult Response XE Mountain Bike 2,2,163]
[127,Stiga Master Series ST3100 Competition Indoor,2,162]
[68,Diamondback Adult Outlook Mountain Bike 2014,2,161]
[61,Diamondback Girls' Clarity 24 Hybrid Bike 201,2,154]
[96,Teeter Hang Ups NXT-S Inversion Table,2,154]
[106,Teeter Hang Ups NXT-S Inversion Table,2,154]
[16,Riddell Youth 360 Custom Football Helmet,2,154]
[58,Diamondback Boys' Insight 24 Performance Hybr,2,154]
[137,Teeter Hang Ups NXT-S Inversion Table,2,154]
[153,Teeter Hang Ups NXT-S Inversion Table,2,154]
[49,Diamondback Adult Sorrento Mountain Bike 2014,2,153]
[144,Garmin Forerunner 220 GPS Watch,2,152]
[52,Easton Mako Youth Bat 2014 (-11),2,151]
[11,Fitness Gear 300 lb Olympic Weight Set,2,148]
[63,Fitness Gear 300 lb Olympic Weight Set,2,148]*/

//find top 10 customers with most unique product purchases. if more than one customer has the same number of product purchases then the customer with the lowest customer_id will take precedence
val top10Cust = sqlContext.sql("""
select o.order_customer_id, count(distinct(oi.order_item_product_id)) as pcount from 
problem6.orders o inner join problem6.order_items oi on (o.order_id =oi.order_item_order_id)
group by order_customer_id order by pcount desc, o.order_customer_id limit 10
""" );
/*[1288,17]                                                                       
[1657,17]
[1810,17]
[12226,17]
[2292,16]
[2403,16]
[3161,16]
[5691,16]
[5715,16]
[8766,16]*/

//On dataset from productRankInDept, apply filter such that only products less than 100 are extracted
val productLess100 = productRankInDept.filter($"product_price" < 100 )
/*[7,Schutt Youth Recruit Hybrid Custom Football H,99.99,2,105]                   
[77,Schutt Youth Recruit Hybrid Custom Football H,99.99,2,105]
[78,Nike Kids' Grade School KD VI Basketball Shoe,99.99,2,105]
[88,Nike Kids' Grade School KD VI Basketball Shoe,99.99,2,105]
[113,Nike Men's Alpha Pro D Mid Football Cleat,99.99,2,105]
[91,Quest Q100 10' X 10' Dome Canopy,99.98,2,101]
[105,Quest Q100 10' X 10' Dome Canopy,99.98,2,101]
[133,Quest Q100 10' X 10' Dome Canopy,99.98,2,101]
[152,Quest Q100 10' X 10' Dome Canopy,99.98,2,101]
[56,Fitbit Flex Wireless Activity & Sleep Wristba,99.95,2,99]*/

// store top100Cust and productLess100 into hive tables
top10Cust.registerTempTable("top10CustTable")
productLess100.registerTempTable("productLess100")
sqlContext.sql("create table top_100_customers as select * from top10CustTable")
sqlContext.sql("create table product_less_100 as select * from productLess100")
