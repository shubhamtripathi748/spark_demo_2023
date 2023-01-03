import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, concat, lag, lit, sum, unix_timestamp, when}
object UserSession {
  def main(args: Array[String]): Unit = {

    val logger: Logger = Logger.getLogger("My.Example.Code.Rules")
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)
    logger.setLevel(Level.ERROR)

    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    import spark.implicits._
    val sessionData= Seq(("2018-01-01 11:00:00", "u1"),
      ("2018-01-01 12:10:00", "u1"),
      ("2018-01-01 13:00:00", "u1"),
      ("2018-01-01 14:40:00", "u1"),
      ("2018-01-01 15:30:00", "u1"),
      ("2018-01-01 15:35:00", "u1"),
      ("2018-01-01 11:00:00", "u2"),
      ("2018-01-02 11:00:00", "u2")).toDF("click-time","user-id")


    val windowPartitionByUser=Window.partitionBy(col("user-id")).orderBy(col("click-time"))

    val datawithlag= sessionData.withColumn("lagvalue",lag(col("click-time"),1).over(windowPartitionByUser))


    val df1=datawithlag.withColumn("tsDiff",
            (unix_timestamp(col("click-time"))-unix_timestamp(col("lagvalue")))/60)
    val df2=df1.withColumn("tsDiff",when(col("tsDiff").isNull,0).otherwise(col("tsDiff")))
    val df3=df2.withColumn("NewSession",when(col("tsDiff")>30,1).otherwise(0))
    val df4=df3.withColumn("Session-Name",
           concat(col("user-id"),lit("--S"),
             sum(col("NewSession")).over(windowPartitionByUser)))
    df4.show()


  }
}
