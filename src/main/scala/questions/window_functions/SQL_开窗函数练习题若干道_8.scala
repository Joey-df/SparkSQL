package questions.window_functions

import org.apache.spark.sql.{DataFrame, SparkSession}

//订单表：order_t
//字段：
//orderid
//userid
//paidtime
//paidfee
//计算用户累计成单达到一定金额的时间
//输出：userid, keypoint, keypoint_time
//keypoint：>1000,>2000,>5000
//源表：
//orderid    userid    paidtime     paidfee
//1           a        2021-04-05    200
//2           a        2021-04-06    200
//3           a        2021-04-07    200
//4           a        2021-04-08    200
//5           a        2021-04-09    300
//6           a        2021-04-11    1000
//目标表：
//userid    keypoint      keypoint_time
//a           >1000               2021-04-09
//a           >2000               2021-04-11
object SQL_开窗函数练习题若干道_8 {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder()
      .master("local")
      .appName("test0")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()
    ss.sparkContext.setLogLevel("ERROR")

    import ss.implicits._

    val df1: DataFrame = List(
      ("1","a","2021-04-05",200),
      ("2","a","2021-04-06",200),
      ("3","a","2021-04-07",100),
      ("4","a","2021-04-08",400),
      ("4","a","2021-04-09",200),
      ("4","a","2021-04-10",800),
      ("5","a","2021-04-11",300),
      ("6","a","2021-04-12",3000),
      ("6","a","2021-04-13",4000)
    ).toDF("orderid","userid","paidtime","paidfee")

    df1.createTempView("order_t")

    ss.sql(
      """
        |select * from order_t
        |""".stripMargin).show()

    ss.sql(
      """
        |select userid,
        |       keypoint,
        |       min(paidtime) keypoint_time
        |from (
        |         select userid,
        |                orderid,
        |                paidtime,
        |                (case
        |                     when cur_sum > 1000 and cur_sum < 2000 then '>1000'
        |                     when cur_sum > 2000 and cur_sum < 5000 then '>2000'
        |                     else '>5000' end) as keypoint
        |         from (
        |                  select userid,
        |                         orderid,
        |                         paidtime,
        |                         sum(paidfee) over (partition by userid order by paidtime asc) cur_sum
        |                  from order_t
        |              ) tmp
        |         where cur_sum > 1000
        |     ) tt
        |where keypoint in ('>1000', '>2000', '>5000')
        |group by userid, keypoint
        |""".stripMargin).show()

  }

}


