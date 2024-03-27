package questions.window_functions

import org.apache.spark.sql.{DataFrame, SparkSession}

//业务背景
//为了提高某店铺的营业额，最近运营部门上线了多个运营活动，用户参与活动后可以领取不定额的优惠券，以此鼓励用户下单。但每个用户限参与一个活动，不可同时参与多个活动。
//数据表介绍
//现有一张订单表orders和活动报名表act_join,分别记录了用户的下单明细和用户报名的活动明细。具体字段如下：
//订单表orders，大概字段有（user_id '用户编号',order_id'订单编号' ，order_sales '订单金额', order_time '下单时间'）。
//活动报名表act_join，大概字段有（act_id '活动编号', user_id '报名用户', join_time '报名时间'）
//
//需求：
//1、统计每个活动报名的所有用户在报名后产生的总订单金额，总订单数。（每个用户限报一个活动,且默认用户报名后产生的订单均为参加活动的订单）。
//2、统计每个活动从开始后到当天（数据统计日）平均每天产生的订单数，活动开始时间定义为最早有用户报名的时间。
object SQL_开窗函数练习题若干道_7 {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder()
      .master("local")
      .appName("test0")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()
    ss.sparkContext.setLogLevel("ERROR")

    import ss.implicits._

    val df1: DataFrame = List(
      ("10001","ord_13361",120,"2021-07-01 07:12:05"),
      ("10002","ord_13247",100,"2021-07-03 07:13:47"),
      ("10003","ord_10924",150,"2021-07-05 09:46:26"),
      ("10005","ord_13071",250,"2021-07-04 09:47:15"),
      ("10006","ord_14533",450,"2021-07-08 09:47:25"),
      ("10007","ord_11134",140,"2021-07-12 09:52:01"),
      ("10008","ord_14305",180,"2021-07-09 18:53:02"),
      ("10009","ord_13055",220,"2021-07-14 18:54:10"),
      ("10011","ord_10460",190,"2021-07-10 18:56:34"),
      ("10012","ord_11959",210,"2021-07-13 18:58:23")
    ).toDF("user_id","order_id","order_sales","order_time")

    df1.createTempView("orders")

    ss.sql(
      """
        |select * from orders
        |""".stripMargin).show()

    val df2: DataFrame = List(
      ("act_01","10001","2021-07-01 07:12:05"),
      ("act_01","10002","2021-07-03 07:13:47"),
      ("act_01","10003","2021-07-05 09:46:26"),
      ("act_01","10005","2021-07-04 09:47:15"),
      ("act_02","10006","2021-07-08 09:47:25"),
      ("act_02","10007","2021-07-12 09:52:01"),
      ("act_02","10008","2021-07-09 18:53:02"),
      ("act_03","10009","2021-07-14 18:54:10"),
      ("act_03","10011","2021-07-10 18:56:34"),
      ("act_03","10012","2021-07-13 18:58:23")
    ).toDF("act_id","user_id","join_time")

    df2.createTempView("act_join")

    ss.sql(
      """
        |select * from act_join
        |""".stripMargin).show()

    // 第1问
    // 计算总订单金额和总订单数，这两个指标都比较简单sum(order_sales)、count(order_id)就可以，
    // 但是关键在于限定条件，是每个活动报名后的用户的汇总，必须是报名了某个活动，且必须在活动开始后的数据统计。
    // 可以通过订单表orders和报名表act_join连接，限定订单时间大于等于活动的开始时间即可。
    ss.sql(
      """
        |select act_id,
        |       sum(order_sales) sum_sales,
        |       count(order_id)  order_cnt
        |from (
        |         select act_id,
        |                t1.user_id,
        |                join_time,
        |                order_id,
        |                order_sales,
        |                order_time
        |         from act_join t1 inner join orders t2 on t1.user_id = t2.user_id
        |         where t2.order_time >= t1.join_time
        |     ) t0
        |group by act_id
        |""".stripMargin)
      .show()

    // 第2问
    // 与第1问有相似之处，同样是用户报名后的下单，
    // 只是多了一些限定条件：同时要满足要小于等于计算日期当天，也就是程序运行的系统时间now()，
    // 在此基础上，计算整体的订单数，除以活动进行的天数，就是该活动每天的平均下单数。
    ss.sql(
      """
        |select t1.act_id,
        |       count(order_id) / datediff(now(), min(t1.begin_time)) --总订单数/活动天数
        |from (
        |         select act_id,
        |                user_id,
        |                join_time,
        |                min(join_time) over (partition by act_id) as begin_time --当前活动的开始时间(最早有用户报名的时间)
        |         from act_join
        |     ) t1
        |         inner join
        |     (
        |         select user_id,
        |                order_id,
        |                order_time
        |         from orders
        |     ) t2
        |     on t1.user_id = t2.user_id
        |where t1.join_time between t1.begin_time and now() --活动开始至今的数据
        |  and t2.order_time >= t1.join_time                --活动开始后的下单
        |group by t1.act_id
        |""".stripMargin)
      .show()
  }

}


