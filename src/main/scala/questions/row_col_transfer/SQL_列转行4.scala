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
    ).toDF("product","tmall","taobao","jd")


    df1.createTempView("table_source")
    ss.sql(
      """
        | select * from table_source
        |""".stripMargin)
      .show()

    //方法1：使用union
    ss.sql(
      """
        |select product, 'tmall' as platform, tmall as price from table_source
        |union
        |select product, 'taobao' as platform, taobao as price from table_source
        |union
        |select product, 'jd' as platform, jd as price from table_source
        |""".stripMargin)
      .show()

    //方法2：使用explode函数
    ss.sql(
      """
        |select product,
        |       platform,
        |       price
        |from (select product,
        |             map("tmall", tmall, "taobao", taobao, "jd", jd) as kv
        |      from table_source) t
        |lateral view outer explode(kv) tf as platform, price;
        |""".stripMargin)
      .show()

  }

}
