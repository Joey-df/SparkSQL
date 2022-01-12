package questions.practice1

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 第三题（尚硅谷第1题）
 * 需求：
 * 我们有如下的用户访问数据，存储在visit表中
 * +-------+----------+-----------+
 * |user_id|visit_date|visit_count|
 * +-------+----------+-----------+
 * |    u01| 2017/1/21|          5|
 * |    u02| 2017/1/23|          6|
 * |    u03| 2017/1/22|          8|
 * |    u04| 2017/1/20|          3|
 * |    u01| 2017/1/23|          6|
 * |    u01| 2017/2/21|          8|
 * |    u02| 2017/1/23|          6|
 * |    u01| 2017/2/22|          4|
 * +-------+----------+-----------+
 *
 * 要求使用SQL统计出每个用户的累积访问次数，如下表所示：
 * +-------+-----------+------------------+---------------+
 * |user_id|visit_month|month_total_visit_cnt|total_visit_cnt|
 * +-------+-----------+------------------+---------------+
 * |    u01|    2017-01|                 11|             11|
 * |    u01|    2017-02|                12|             23|
 * |    u02|    2017-01|                 12|             12|
 * |    u03|    2017-01|                 8|              8|
 * |    u04|    2017-01|                  3|              3|
 * +-------+-----------+------------------+---------------+
 */
//考察函数：
//regexp_replace(visit_date,'/','-')
//sum + group by
//开窗函数
object Problem_0003 {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder()
      .master("local")
      .appName("test")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    ss.sparkContext.setLogLevel("ERROR")

    import ss.implicits._

    val df1: DataFrame = List(
      ("u01","2017/1/21",5),
      ("u02","2017/1/23",6),
      ("u03","2017/1/22",8),
      ("u04","2017/1/20",3),
      ("u01","2017/1/23",6),
      ("u01","2017/2/21",8),
      ("u02","2017/1/23",6),
      ("u01","2017/2/22",4)
    ).toDF("user_id","visit_date","visit_count")

    df1.createTempView("visit")

    ss.sql(
      """
        |select user_id,
        |       visit_month,
        |       month_total_visit_cnt,
        |       sum(month_total_visit_cnt) over (partition by user_id order by visit_month) total_visit_cnt
        |from (
        |         select user_id,
        |                visit_month,
        |                sum(visit_count) month_total_visit_cnt
        |         from (select user_id,
        |                      regexp_replace(visit_date, '/', '-')                            visit_date1,
        |                      date_format(regexp_replace(visit_date, '/', '-'), 'yyyy-MM') as visit_month,
        |                      visit_count
        |               from visit) t1
        |         group by user_id, visit_month
        |         order by user_id, visit_month
        |     ) t2
        |order by t2.user_id, t2.visit_month;
        |""".stripMargin)
      .show()

  }

}
