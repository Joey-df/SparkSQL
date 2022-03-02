package questions.window_functions

import org.apache.spark.sql.{DataFrame, SparkSession}

//用户抽奖行为分析
//业务背景
//某购物APP最近上线了一个新功能，用户签到后可以跳转到大转盘抽奖，抽奖获得的奖金可以抵消购物的费用，以此来培养用户使用app的习惯。
//
//数据表介绍
//现有一张用户行为表user_log，记录了用户在app上的所有行为日志，
//主要字段如下，即何人user_id在何时event_time进行了什么操作event_id。
//
//需求：
//1、计算该app每天的访客数、以及每天人均行为次数。
//2、统计每天签到之后并进行抽奖的用户数，注意签到和抽奖行为必须相邻（签到和抽奖行为对应的event_id分别为'register','gift'）。
object SQL_开窗函数练习题若干道_6 {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder()
      .master("local")
      .appName("test0")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()
    ss.sparkContext.setLogLevel("ERROR")

    import ss.implicits._

    val df1: DataFrame = List(
      ("10001","2021-01-01 07:12:05","login"),
      ("10001","2021-01-01 07:13:47","register"),
      ("10001","2021-01-02 09:46:26","login"),
      ("10001","2021-01-02 09:47:15","register"),
      ("10001","2021-01-02 09:47:25","gift"),
      ("10001","2021-01-02 09:52:01","logout"),
      ("10002","2021-01-03 18:53:02","login"),
      ("10002","2021-01-03 18:54:10","view"),
      ("10002","2021-01-03 18:56:34","cart"),
      ("10002","2021-01-03 18:58:23","check")
    ).toDF("user_id","event_time","event_id")

    df1.createTempView("user_log")

    ss.sql(
      """
        |select * from user_log
        |""".stripMargin).show()

    //第1问
    ss.sql(
      """
        |select substring(event_time, 1, 10) as           date,
        |       count(distinct user_id)                   user_cnt,   -- 每天的访客数
        |       count(event_id) / count(distinct user_id) avg_opr_cnt -- 每天人均行为次数
        |from user_log
        |group by substring(event_time, 1, 10)
        |""".stripMargin)
      .show()

    //或者
    ss.sql(
      """
        |select cast(event_time as date)                  as day,
        |       count(distinct user_id)                   as active_cnt,
        |       count(event_id) / count(distinct user_id) as avg_opr_cnt
        |from user_log
        |group by cast(event_time as date)
        |""".stripMargin)
        .show()


    //第2问
    ss.sql(
      """
        |select date,
        |       count(distinct user_id)
        |from (
        |         select user_id,
        |                substring(event_time, 1, 10) as                                           date,
        |                event_time,
        |                event_id,
        |                lead(event_id, 1, null)
        |                     over (partition by substring(event_time, 1, 10) order by event_time) next_event_id
        |         from user_log
        |     ) tmp
        |where event_id = 'register' and next_event_id = 'gift'
        |group by date
        |""".stripMargin)
      .show()
  }

}


