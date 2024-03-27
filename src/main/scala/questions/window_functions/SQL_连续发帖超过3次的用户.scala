package questions.window_functions

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 问题：
 * 一个表记录了某论坛会员的发贴情况，存储了会员uid，发贴时间post_time和内容content。
 * 找出连续发贴三次及以上的会员。
 */
object SQL_连续发帖超过3次的用户 {

  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder()
      .master("local")
      .appName("test")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    ss.sparkContext.setLogLevel("ERROR")

    import ss.implicits._

    val df1: DataFrame = List(
      (1,"2019-03-01 00:00:00","a"),
      (2,"2019-03-01 00:00:01","b"),
      (3,"2019-03-01 00:00:02","c"),
      (3,"2019-03-01 00:00:03","d"),
      (3,"2019-03-01 00:00:04","e"),
      (2,"2019-03-01 00:00:05","f"),
      (2,"2019-03-01 00:00:06","g"),
      (1,"2019-03-01 00:00:07","h"),
      (4,"2019-03-01 00:00:08","i"),
      (4,"2019-03-01 00:00:09","j"),
      (4,"2019-03-01 00:00:10","k"),
      (5,"2019-03-01 00:00:11","l")
    ).toDF("user_id","post_time","content")

    df1.createTempView("log_info")

    ss.sql(
      """
        |select user_id,
        |       (postTime - rn) as timediff,
        |       count(1) as times
        |from (
        |         select user_id,
        |                post_time,
        |                unix_timestamp(post_time, "yyyy-MM-dd HH:mm:ss")            as postTime,
        |                row_number() over (partition by user_id order by post_time) as rn
        |         from log_info
        |     ) t1
        |group by user_id, (postTime - rn)
        |having count(1) >= 3
        |""".stripMargin)
      .show()

    ss.sql(
      """
        |select user_id,post_time,
        |       unix_timestamp(post_time, "yyyy-MM-dd HH:mm:ss")            as postTime,
        |       row_number() over (partition by user_id order by rand()) as rn
        |from log_info
        |""".stripMargin)
      .show()
  }

}
