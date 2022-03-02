package questions.window_functions

import org.apache.spark.sql.SparkSession

object SQL_开窗函数练习题若干道_1 {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder()
      .master("local")
      .appName("test0")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()
    ss.sparkContext.setLogLevel("ERROR")

    //name,orderdate,cost
    //jack,2017-01-01,10
    //tony,2017-01-02,15
    //jack,2017-02-03,23
    //tony,2017-01-04,29
    //jack,2017-01-05,46
    //jack,2017-04-06,42
    //tony,2017-01-07,50
    //jack,2017-01-08,55
    //mart,2017-04-08,62
    //mart,2017-04-09,68
    //neil,2017-05-10,12
    //mart,2017-04-11,75
    //neil,2017-06-12,80
    //mart,2017-04-13,94
    ss.read.option("header", true).csv("./data/business.csv")
      .createTempView("business")

    ss.sql(
      """
        | select * from business
        |""".stripMargin)
      .show()

    //查询2017年4月份购买的详细信息
    ss.sql(
      """
        | SELECT *
        | FROM business
        | WHERE SUBSTRING(orderdate, 1, 7) = '2017-04'
        |""".stripMargin)
      .show()

    //查询2017年4月份购买的顾客（要求名字不能重复）
    ss.sql(
      """
        | SELECT name
        | FROM business
        | WHERE SUBSTRING(orderdate, 1, 7) = '2017-04'
        | group by name
        |""".stripMargin)
      .show()

    //查询2017年4月份，每个顾客购买的次数
    ss.sql(
      """
        | SELECT name, COUNT(*)
        | FROM business
        | WHERE SUBSTRING(orderdate, 1, 7) = '2017-04'
        | GROUP BY name
        |""".stripMargin)
      .show()

    //查询在2017年4月份购买过的顾客及总人数
    ss.sql(
      """
        | SELECT name, COUNT(*) OVER ()
        | FROM business
        | WHERE SUBSTRING(orderdate, 1, 7) = '2017-04'
        | GROUP BY name
        |""".stripMargin)
      .show()


    //查询顾客的购买明细及月购买总额
    ss.sql(
      """
        |select name,
        |       orderdate,
        |       cost,
        |       sum(cost) over (partition by month(orderdate) )   monthSum1, --可以理解为group by + sum
        |       sum(cost) over (partition by month(orderdate) order by orderdate rows between unbounded preceding and unbounded following) monthSum2,
        |       sum(cost) over (partition by name,month(orderdate) order by orderdate rows between unbounded preceding and unbounded following) userMonthSum
        |from business
        |""".stripMargin).show()

    //查询顾客的购买明细及到目前为止每个顾客购买总金额
    ss.sql(
      """
        |select name,
        |       orderdate,
        |       cost,
        |       sum(cost) over() as sample1,--所有行相加（整张表作为一个大窗口）
        |       sum(cost) over(partition by name) as sample2,--按name分组，组内数据相加，类似与group by name + sum(cost)
        |       sum(cost) over(partition by name order by orderdate) as sample3,--按name分组，组内数据累加
        |       sum(cost) over(partition by name order by orderdate rows between UNBOUNDED PRECEDING and current row ) as sample4 ,--和sample3一样,由起点到当前行的聚合
        |       sum(cost) over(partition by name order by orderdate rows between 1 PRECEDING and current row) as sample5, --当前行和前面一行做聚合
        |       sum(cost) over(partition by name order by orderdate rows between 1 PRECEDING AND 1 FOLLOWING ) as sample6,--当前行和前边一行及后面一行
        |       sum(cost) over(partition by name order by orderdate rows between current row and UNBOUNDED FOLLOWING ) as sample7 --当前行及后面所有行
        |from business
        |""".stripMargin).show()

    //查询顾客上次的购买时间
    ss.sql(
      """
        |select name,
        |       orderdate,
        |       cost,
        |       lag(orderdate,1,NULL) over(partition by name order by orderdate) as last_date
        |from business
        |""".stripMargin).show()

    //查询前20%时间的订单信息----ntile(n)
    ss.sql(
      """
        |select *
        |from (
        |         select name,
        |                orderdate,
        |                cost,
        |                ntile(5) over (order by orderdate) as bucket
        |         from business
        |     ) tmp
        |where tmp.bucket = 1
        |""".stripMargin).show()

    ss.sql(
      """
        |select name,
        |       orderdate,
        |       cost,
        |       ntile(3) over (order by cost)                    as bucket3, -- 全局按照cost升序排列,数据切成3份
        |       ntile(3) over (partition by name order by cost ) as bucket4  -- 按照name分组，在分组内按照cost升序排列,数据切成3份
        |from business
        |""".stripMargin).show()


    //6）求每位顾客当前购买日期与最开始购买日期的时间间隔
    ss.sql(
      """
        |select name,
        |       orderdate,
        |       cost,
        |       datediff(orderdate, fv) diff1,
        |       datediff(cur_lv, fv)    diff2
        |from (
        |         select name,
        |                orderdate,
        |                cost,
        |                first_value(orderdate) over (partition by name order by orderdate) fv,
        |                last_value(orderdate) over (partition by name order by orderdate)  cur_lv
        |         from business
        |     ) tmp
        |""".stripMargin).show()

  }

}


