package questions.window_functions

import org.apache.spark.sql.{DataFrame, SparkSession}

//现有一张表 score_info 记录了一场篮球比赛中各个球员的得分记录，
//即某个球员userid得分了，就记录该球员的得分时间score_time和得分score。
//
//需求：计算连续3次得分的用户数，而且中间不能有别的球员得分。
//分析：本题的难点在于需要构造一个可以相减的字段
object SQL_开窗函数练习题若干道_5 {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder()
      .master("local")
      .appName("test0")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()
    ss.sparkContext.setLogLevel("ERROR")

    import ss.implicits._

    val df1: DataFrame = List(
      ("A0001","t1",2),
      ("A0001","t2",2),
      ("A0002","t3",3),
      ("A0001","t4",2),
      ("A0001","t5",2),
      ("A0001","t6",2),
      ("A0003","t7",3),
      ("A0003","t8",2)
    ).toDF("user_id","score_time","score")

    df1.createTempView("score_info")

    ss.sql(
      """
        |select * from score_info
        |""".stripMargin).show()

    //debug
    ss.sql(
      """
        |select *,
        |    row_number() over( order by score_time) as rn_all,
        |    row_number() over( partition by user_id order by score_time) as rn_user
        |from score_info
        |""".stripMargin).show()

    ss.sql(
      """
        |select user_id, diff, count(1)
        |from (select *, rn_all - rn_user as diff
        |      from (select *,
        |                row_number() over( order by score_time) as rn_all,
        |                row_number() over( partition by user_id order by score_time) as rn_user
        |            from score_info
        |            ) a
        |      ) b
        |group by user_id, diff
        |having count(1) >= 3
        |""".stripMargin).show()
  }

}


