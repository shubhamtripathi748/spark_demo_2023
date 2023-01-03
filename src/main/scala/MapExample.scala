import org.apache.spark.sql.SparkSession

object MapExample {
  def main(args: Array[String]): Unit = {
  val  spark= SparkSession.builder().appName("mapPartitions").master("local[*]").getOrCreate()
  val lst=List(10,20,30,10,20,30,10,20,30,10,20,30,10,20,30,10,20,30,10,20,30
    ,10,20,30,10,20,30,10,20,30, 10, 20, 30, 10, 20, 30, 10, 20, 30, 10, 20, 30, 10, 20, 30, 10, 20, 30, 10, 20, 30
    , 10, 20, 30, 10, 20, 30, 10, 20, 30, 10, 20, 30, 10, 20, 30, 10, 20, 30, 10, 20, 30, 10, 20, 30, 10, 20, 30, 10, 20, 30
    , 10, 20, 30, 10, 20, 30, 10, 20, 30, 10, 20, 30, 10, 20, 30, 10, 20, 30, 10, 20, 30, 10, 20, 30, 10, 20, 30, 10, 20, 30
    , 10, 20, 30, 10, 20, 30, 10, 20, 30)
     val df= spark.sparkContext.parallelize(lst)
      df.repartition(4).mapPartitions(x=>Iterator(x.length)).collect()
  }
}
