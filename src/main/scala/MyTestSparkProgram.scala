import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{collect_set, count, dense_rank, row_number}

object MyTestSparkProgram {
  def main(Args:Array[String])={
    val logger: Logger = Logger.getLogger("My.Example.Code.Rules")
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)
    logger.setLevel(Level.ERROR)

    val spark=SparkSession.builder().appName("MyFirstProgram").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
   /* val data=Seq(("abhi","31"),("shubham","33"))
    import spark.implicits._
    val df=data.toDF("name","id")
    df.show()*/

    val sc=spark.sparkContext
   /* val textrdd=sc.textFile("abc.txt")
    val wordMap=textrdd.flatMap(line=>line.split(" ")).map(word=>(word,1)).reduceByKey(_+_).collect()
  wordMap.foreach(println)
*/

    //drop duplicate and distinct function in dataframe
    val duplicatedf=spark.read.format("csv").option("delimiter","|").option("header",true).option("inferSchema",true).load("duplicate.csv").coalesce(5)
    duplicatedf.distinct().show()
   println(duplicatedf.count())

    import org.apache.spark.sql.functions.{col,countDistinct}
    duplicatedf.dropDuplicates("Name","Age").show()

    duplicatedf.createOrReplaceTempView("duplicateDFTable")

    //countDistinct
    duplicatedf.selectExpr("count('Year') as CountYear").show()
    duplicatedf.select(countDistinct("Year")).show()

    import org.apache.spark.sql.functions.{max,min}
    duplicatedf.select(max("Year"),min("Year")).show()


    val titanic= spark.read.format("csv").option("delimiter", ",").
                        option("header", true).option("inferSchema", true).
                            load("titanic.csv").coalesce(5)


    import org.apache.spark.sql.functions.{aggregate}
    titanic.createOrReplaceTempView("titanic")
    //group By example
    titanic.groupBy("Sex","Pclass").
      agg(count("PassengerId").alias("CountPassen"))
      .show()

    //Windows function==>dense rank,rank,row num,first,last,lead,lag,
    val windowBySexInDesc=Window.partitionBy("Sex")
      .orderBy(col("Sex").desc)
        .rowsBetween(Window.unboundedPreceding,Window.currentRow)
    val titanicWithNotNull=titanic.drop()
    val rank= dense_rank().over(windowBySexInDesc)
    val rowNum=row_number().over(windowBySexInDesc)
    titanicWithNotNull.withColumn("rank",rank)
      .withColumn("row_num",rowNum)
      .select("Sex","Name","rank","row_num").where("row_num=1").show()


  }
}
