package questions.window_functions

import org.apache.spark.sql.{DataFrame, SparkSession}

//假设有一个网店，上线了100多个商品，每个顾客浏览任何一个商品时都会产生一条浏览记录，
//浏览记录存储的表名为product_view，访客的用户id为user_id，浏览的商品名称是product_id。
//需求：每个商品浏览次数top3 的 用户信息，输出商品id、用户id、浏览次数。
object SQL_开窗函数练习题若干道_4 {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder()
      .master("local")
      .appName("test0")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()
    ss.sparkContext.setLogLevel("ERROR")

    import ss.implicits._

    val df1: DataFrame = List(
      ("A0001","product_a"),
      ("A0002","product_b"),
      ("A0001","product_b"),
      ("A0001","product_a"),
      ("A0003","product_c"),
      ("A0004","product_b"),
      ("A0001","product_a"),
      ("A0002","product_c"),
      ("A0005","product_b"),
      ("A0004","product_b"),
      ("A0006","product_c"),
      ("A0002","product_c"),
      ("A0001","product_b"),
      ("A0002","product_a"),
      ("A0002","product_a"),
      ("A0003","product_a"),
      ("A0005","product_a"),
      ("A0005","product_a"),
      ("A0005","product_a")
    ).toDF("user_id","product_id")

    df1.createTempView("product_view")

    ss.sql(
      """
        |select * from product_view
        |""".stripMargin).show()

  }

}


