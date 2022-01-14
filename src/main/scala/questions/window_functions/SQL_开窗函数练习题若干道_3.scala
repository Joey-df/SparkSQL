package questions.window_functions

import org.apache.spark.sql.{DataFrame, SparkSession}

//1、使用 over() 函数进行数据统计, 统计每个用户及表中数据的总数
//2、求用户明细并统计每天的用户总数
//3、计算从第一天到现在的所有 score 大于80分的用户总数
//4、计算每个用户到当前日期分数大于80的天数
object SQL_开窗函数练习题若干道_3 {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder()
      .master("local")
      .appName("test0")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()
    ss.sparkContext.setLogLevel("ERROR")

    import ss.implicits._

    val df1: DataFrame = List(
      ("孙悟空","语文",87),
      ("孙悟空","数学",95),
      ("孙悟空","英语",68),
      ("大海","语文",94),
      ("大海","数学",56),
      ("大海","英语",84),
      ("宋宋","语文",64),
      ("宋宋","数学",86),
      ("宋宋","英语",84),
      ("婷婷","语文",65),
      ("婷婷","数学",85),
      ("婷婷","英语",78)
    ).toDF("name","subject","score")

    df1.createTempView("score_info")

    ss.sql(
      """
        |select * from score_info
        |""".stripMargin).show()

  }

}


