package questions.row_col_transfer

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 考察函数：
 *  map函数
 *  union
 *  lateral view explode
 */
object SQL_列转行4 {

  def main(args: Array[String]): Unit = {


    val ss = SparkSession.builder()
      .master("local")
      .appName("test0")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()
    ss.sparkContext.setLogLevel("ERROR")

    import ss.implicits._

    val df1: DataFrame = List(
      ("产品A", 90, 85, 87),
      ("产品B", 81, 86, 98),
      ("产品C", 79, 92, 93)
    ).toDF("Product","Tmall","taobao","JD")


    df1.createTempView("table_source")
    ss.sql(
      """
        | select * from table_source
        |""".stripMargin)
      .show()

    //方法1：使用union
    ss.sql(
      """
        |
        |""".stripMargin)
      .show()

    //方法2：使用explode函数
    ss.sql(
      """
        |
        |""".stripMargin)
      .show()

  }

}
