package questions.window_functions

import org.apache.spark.sql.{DataFrame, SparkSession}

//1、使用 over() 函数进行数据统计, 统计每个用户及表中数据的总数
//2、求用户明细并统计每天的用户总数
//3、计算从第一天到现在的所有 score 大于80分的用户总数
//4、计算每个用户到当前日期分数大于80的天数
object SQL_开窗函数练习题若干道_2 {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder()
      .master("local")
      .appName("test0")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()
    ss.sparkContext.setLogLevel("ERROR")

    import ss.implicits._

    val df1: DataFrame = List(
      ("20191020","11111",85),
      ("20191020","22222",83),
      ("20191020","33333",86),
      ("20191021","11111",87),
      ("20191021","22222",65),
      ("20191021","33333",98),
      ("20191022","11111",67),
      ("20191022","22222",34),
      ("20191022","33333",88),
      ("20191023","11111",99),
      ("20191023","22222",33)
    ).toDF("logday","userid","score")

    df1.createTempView("log_info")

    ss.sql(
      """
        |select * from log_info
        |""".stripMargin).show()

  }

}


