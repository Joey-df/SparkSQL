package questions.window_functions

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 源表 sale_info
 * date     sale
 * 2021-10-01   200
 * 2021-10-03   500
 * 2021-10-05   900
 * 2021-10-10   600
 * 2021-11-05   800
 * 2021-11-19   612
 *
 * 目标表
 * date     sale
 * 2021-10-01   200
 * 2021-10-03   700
 * 2021-10-05   1600
 * 2021-10-10   2200
 * 2021-11-05   800
 * 2021-11-19   1412
 */
object SQL_按月统计销售额 {

  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder()
      .master("local")
      .appName("test")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    ss.sparkContext.setLogLevel("ERROR")

    import ss.implicits._

    val df1: DataFrame = List(
      ("2021-10-01",200),
      ("2021-10-03",500),
      ("2021-10-05",900),
      ("2021-10-10",600),
      ("2021-11-05",800),
      ("2021-11-19",612)
    ).toDF("date","sale")

    df1.createTempView("sale_info")
    ss.sql(
      """
        | select * from sale_info
        |""".stripMargin)
      .show()

    //考察函数
    //month(date)
    //substr(date, beginIndex, endIndex)
    //substring(date, beginIndex, endIndex)
    ss.sql(
      """
        | select
        |    date,
        |    sum(sale) over( partition by month(date) order by date) as cur_sum,
        |    sum(sale) over( partition by substr(date,1,7) order by date) as cur_sum2,
        |    sum(sale) over( partition by substring(date,1,7) order by date) as cur_sum3
        | from sale_info
        |""".stripMargin)
      .show()
  }
}
