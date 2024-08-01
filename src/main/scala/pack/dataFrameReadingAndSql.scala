package pack
import org.apache.spark.SparkContext  // rdd
import org.apache.spark.sql.SparkSession  // dataframe
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions.upper
import org.apache.spark.sql.catalyst.expressions.Upper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import scala.io.Source
object dataFrameReadingAndSql {
  def main(args:Array[String]):Unit= {
    println("===started===")
    println

    val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf) // RDD

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate() //Dataframe

    import spark.implicits._


    val df = spark.read.format("csv").option("header", "true").load("file:///Users/niranjankashyap/Downloads/Big Data downloads/Data/df.csv")

    val df1 = spark.read.format("csv").option("header", "true").load("file:///Users/niranjankashyap/Downloads/Big Data downloads/Data/df1.csv")

    val prod = spark.read.format("csv").option("header", "true").load("file:///Users/niranjankashyap/Downloads/Big Data downloads/Data/prod.csv")

    val cust = spark.read.format("csv").option("header", "true").load("file:///Users/niranjankashyap/Downloads/Big Data downloads/Data/cust.csv")


    df.show()
    df1.show()
    prod.show()
    cust.show()

    df.createOrReplaceTempView("df")
    df1.createOrReplaceTempView("df1")
    prod.createOrReplaceTempView("prod")
    cust.createOrReplaceTempView("cust")

    println("============Validate the data==============")

    spark.sql("select * from df").show()

    println("===========Select two columns===============")

    spark.sql("select id,tdate from df").show()

    println("===============category filter = Exercise =============")

    spark.sql("select id,tdate,category from df where category = 'Exercise'").show()

    println("===================Multi column filter=================")

    spark.sql("select id,tdate,category,spendby from df where category='Exercise' and spendby='cash'").show()

    println("====================Multi Value filter==================")

    spark.sql("select * from df where category in ('Exercise','Gymnastics')").show()

    println("=======================Like Filter======================")

    spark.sql("select * from df where product like ('%Gym%')").show()

    println("=======================Not Filter=======================")

    spark.sql("select * from df where category != 'Exercise'").show()

    println("=======================Not in Filter======================")

    spark.sql("select * from df where category not in ('Exercise','Gymnastics')").show()

    println("=======================Null Filter=========================")

    spark.sql("select * from df where product is null").show()

    println("=======================Not Null Filter=======================")

    spark.sql("select * from df where product is not null").show()

    println("==========================Max Function=====================")

    spark.sql("select max(id) from df").show()

    println("=========================Min Function=======================")

    spark.sql("select min(id) from df").show()

    println("==========================Count Function=====================")

    spark.sql("select count(id) from df").show()

    println("=========================Condition Statement===================")

    spark.sql("select *,case when spendby='cash' then 1 else 0 end  as status from df  ").show()

    println("==========================Concat data==========================")

    spark.sql("select id,category,concat(id,'-',category) as concat from df").show()

    println("===========================Concat_WS - White Space============== ")

    spark.sql("select id,category,concat_ws('-',id,category,spendby) as concat from df").show()

    println("========================Lower case===============================")

    spark.sql("select id, lower(category) as Lower from df").show()

    println("======================Ceil=======================================")

    spark.sql("select amount,ceil(amount) as ceilvalues from df").show()

    println("============================Round==================================")

    spark.sql("select amount,round(amount) as roundofamount from df").show()

    println("============================replace Nulls======================= ")

    spark.sql("select product,coalesce(product,'NA') as nullrep from df").show()

    println("==============================Trim=================================")

    spark.sql("select trim(product) from df").show()

    println("===============================Distinct============================== ")

    spark.sql("select distinct category,spendby from df").show()

    println("=========================Substring with Trim===========================")

    spark.sql("select substring(product,1,10) as sub from df").show()

    println("===========================Substring/Split operation====================")

    spark.sql("select SUBSTRING_INDEX(category,'',1) as spl from df").show()

    println("============================Union all===================================")

    spark.sql("select * from df union all select * from df1").show()

    println("============================Union=======================================")

    spark.sql("select * from df union select * from df1 order by id").show()

    println("===========================Aggregate Sum================================")

    spark.sql("select category,sum(amount) as total from df group by category").show()

    println("=============================Aggregate sum with two columns================")

    spark.sql("select category,spendby,sum(amount)  as total from df group by category,spendby").show()

    println("=============================Aggregate Count===============================")

    spark.sql("select category,spendby,sum(amount) As total,count(amount)  as cnt from df group by category,spendby").show()

    println("============================Aggregate Max==============================")

    spark.sql("select category, max(amount) as max from df group by category").show()

    println("============================Aggregate with order descending==============================")

    spark.sql("select category, max(amount) as max from df group by category order by category desc").show()

    println("========================Window Row Number==================================")

    spark.sql("SELECT category,amount, row_number() OVER ( partition by category order by amount desc ) AS row_number FROM df").show()

    println("==========================Window Dense_rank Number==========================")

    spark.sql("select category,amount,dense_rank() over(partition by category order by amount desc) as dense_rank from df").show()

    println("==========================Window Rank Number==========================")

    spark.sql("select category,amount,rank() over(partition by category order by amount desc) as rank from df").show()

    println("============================Window Lead function==========================")

    spark.sql("SELECT category,amount, lead(amount) OVER ( partition by category order by amount desc ) AS lead FROM df").show()

    println("==============================Window lag  function==========================")

    spark.sql("SELECT category,amount, lag(amount) OVER ( partition by category order by amount desc ) AS lag FROM df").show()

    println("==============================Having Function=============================")

    spark.sql("select category,count(category) as cnt from df group by category having count(category)>1").show()

    println("=================================Inner Join================================")

    spark.sql("select a.id,a.name,b.product from cust a join prod b on a.id=b.id").show()

    println("================================Left Join==================================")

    spark.sql("select a.id,a.name,b.product from cust a left join prod b on a.id=b.id").show()

    println("================================Right Join==================================")

    spark.sql("select a.id,a.name,b.product from cust a right join prod b on a.id=b.id").show()

    println("================================Full Join==================================")

    spark.sql("select a.id,a.name,b.product from cust a full join prod b on a.id=b.id").show()

    println("================================Left Anti Join==================================")

    spark.sql("select a.id,a.name from cust a left anti join prod b on a.id=b.id").show()

    println("===============================Date Format===============================")

    spark.sql("select id,tdate,from_unixtime(unix_timestamp(tdate,'MM-dd-yyyy'),'yyyy-MM-dd') as con_date from df").show()

    println("==============================Sub Query================================")

    spark.sql("select sum(amount) as total , con_date from(select id,tdate,from_unixtime(unix_timestamp(tdate,'MM-dd-yyyy'),'yyyy-MM-dd') as con_date,amount,category,product,spendby from df) group by con_date").show()
























  }

}
