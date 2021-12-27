package questions

import org.apache.spark.sql.SparkSession

object SQL_Test_business {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder()
      .master("local")
      .appName("test0")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()
    ss.sparkContext.setLogLevel("ERROR")

    ss.read.option("header", true).csv("./data/business.csv")
      .createTempView("business")

    ss.sql(
      """
        | select * from business
        |""".stripMargin)
      .show()

    ss.sql(
      """
        | SELECT *
        | FROM business
        | WHERE SUBSTRING(orderdate, 1, 7) = '2017-04'
        |""".stripMargin)
      .show()

    ss.sql(
      """
        | SELECT name, COUNT(*) OVER ()
        | FROM business
        | WHERE SUBSTRING(orderdate, 1, 7) = '2017-04'
        | GROUP BY name
        |""".stripMargin)
      .show()
  }
}
