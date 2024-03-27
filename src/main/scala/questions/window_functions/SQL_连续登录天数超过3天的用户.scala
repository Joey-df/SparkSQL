package questions.window_functions

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 数据分析经常遇到类似的问题，
 * 比如"统计连续N天交易额超过100万的店铺"、"统计连续登录天数超过3天的用户"等。
 * 对于这类问题，思路基本都是一样的。本题将介绍常用的两种解决方案。
 */
//以"统计连续登录天数超过3天的用户"为需求。
object SQL_连续登录天数超过3天的用户 {

  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder()
      .master("local")
      .appName("test")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    ss.sparkContext.setLogLevel("ERROR")

    import ss.implicits._

    val df1: DataFrame = List(
      ("01","2021-02-28"),
      ("01","2021-03-01"),
      ("01","2021-03-02"),
      ("01","2021-03-04"),
      ("01","2021-03-05"),
      ("01","2021-03-06"),
      ("01","2021-03-07"),
      ("01","2021-03-08"),
      ("02","2021-03-01"),
      ("02","2021-03-02"),
      ("02","2021-03-03"),
      ("02","2021-03-06"),
      ("03","2021-03-06")
    ).toDF("user_id","login_date")

    df1.createTempView("login_info")

    //方案一
    //1.先把数据按照用户id分组，根据登录日期排序
    //2.用登录日期与rn求date_sub，得到的差值日期如果是相等的，则说明这两天肯定是连续的
    //3.根据id和日期差date_diff分组，登录次数即为分组后的count(1)
    ss.sql(
      """
        | SELECT t2.user_id,
        |        t2.diff_date,
        |        count(1)           as login_times, -- 连续登陆天数
        |        min(t2.login_date) as start_date,
        |        max(t2.login_date) as end_date
        | FROM (
        |          SELECT t1.user_id,
        |                 t1.login_date,
        |                 date_sub(t1.login_date, rn) as diff_date
        |          FROM (
        |                   SELECT user_id,
        |                          login_date,
        |                          row_number() over (partition by user_id order by login_date asc) as rn
        |                   FROM login_info
        |               ) t1 --1.先把数据按照user_id分组，根据登录日期升序排序
        |      ) t2
        | group by t2.user_id, t2.diff_date
        | having login_times >= 3
        |
        |""".stripMargin)
      .show()

    //方案二
    //利用lag和lead函数进行处理，思路类似。
    ss.sql(
      """
        | select user_id,
        |        min(lag_login_date) first_start_date
        | from (
        |          SELECT user_id,
        |                 lag_login_date,
        |                 login_date,
        |                 lead_login_date
        |          FROM (
        |                   SELECT user_id,
        |                          login_date,
        |                          lag(login_date, 1, login_date) over (partition by user_id order by login_date asc) as lag_login_date,
        |                          lead(login_date, 1, login_date) over (partition by user_id order by login_date asc) as lead_login_date
        |                   FROM login_info
        |               ) t1
        |          where datediff(login_date, lag_login_date) = 1
        |            and datediff(lead_login_date, login_date) = 1 -- 这两个条件刚好限制了连续3天
        |      ) tmp
        | group by user_id
        |""".stripMargin)
      .show()
  }

}
