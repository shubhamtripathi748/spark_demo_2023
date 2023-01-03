import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}

object EmployeeProgram {
  def main(args: Array[String]): Unit = {

    val session= SparkSession.builder().master("local[*]").appName("EmployeeData").getOrCreate()



    val employeeDf= session.read.option("header",true).option("inferSchema",true).format("csv").load("Employee.csv")
  employeeDf.show()
    import org.apache.spark.sql.functions.{dense_rank,col}
    employeeDf.createOrReplaceTempView("employeeDf")
   val windowPartitionBySal= Window.partitionBy("deptno").orderBy(col("sal").desc)

    employeeDf.withColumn("dense_rank",dense_rank() over windowPartitionBySal).filter("dense_rank=1").show()
    val windowBySal=Window.orderBy(col("sal").desc)
    employeeDf.withColumn("dense", dense_rank() over windowBySal).filter("dense=1").show()

  }
}
