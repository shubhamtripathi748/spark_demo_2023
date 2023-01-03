import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode_outer, split}

object ExplodeExample {

  def main(args: Array[String]): Unit = {

    val logger: Logger = Logger.getLogger("My.Example.Code.Rules")
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)
    logger.setLevel(Level.ERROR)

    val session=SparkSession.builder().master("local[*]").getOrCreate()
      val fileData= session.read.format("csv").option("delimiter","|").option("header",true).option("inferSchema",true)
        .load("MultiDelimiter.csv")
import org.apache.spark.sql.functions.{explode,col}
    fileData.withColumn("explode_Education",explode(split(col("Edu"),",")))
      .drop("Edu").show()

    fileData.withColumn("explode_outer_Education", explode_outer(split(col("Edu"), ",")))
      .drop("Edu").show()

    //explodeData_file.write.format("avro").mode("overwrite").save("D://spark//data_set")

  }
}
