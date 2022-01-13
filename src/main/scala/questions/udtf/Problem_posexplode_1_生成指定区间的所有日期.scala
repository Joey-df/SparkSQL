package questions.udtf

import org.apache.spark.sql.SparkSession

object Problem_posexplode_1_生成指定区间的所有日期 {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder()
      .master("local")
      .appName("test")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    ss.sparkContext.setLogLevel("ERROR")

    //生成指定日期区间的所有日期
    ss.sql(
      """
        |select pos,
        |       x, -- 其实是个空格
        |       date_add('2020-11-01', pos) as dynamic_date,
        |       '2020-11-01'                as start_time,
        |       '2020-11-30'                as end_time
        |from (select 0) t
        |         lateral view
        |             posexplode(split(space(datediff('2020-11-30', '2020-11-01')), ' ')) tf as pos, x
        |""".stripMargin).show(100)


    //测试posexplode array
    ss.sql(
      """
        |select tf.* from (select 0) t
        |     lateral view posexplode(array('A','B','C')) tf as pos,val
        |""".stripMargin).show()

    //测试posexplode map
    ss.sql(
      """
        |select tf.* from (select 0) t
        |     lateral view posexplode(map('A',10,'B',20,'C',30)) tf as pos,k,v
        |""".stripMargin).show()

    //测试 行转列，并把索引取出
    ss.sql(
      """
        |select tf.* from (select 0) t
        |     lateral view posexplode(split(repeat("1#",5), "#")) tf as pos, val
        |     -- where val = "1"
        |""".stripMargin).show()

    //计算指定日期区间之内的所有月份
    ss.sql(
      """
        |SELECT pos,
        |       date_format(add_months(start_date, pos), 'yyyy-MM') AS year_month,
        |       start_date,
        |       end_date
        |FROM (SELECT '2020-10-03' as start_date,
        |             '2021-07-20' as end_date) t
        |         lateral VIEW posexplode(split(space(months_between(end_date, start_date)), ' ')) tf AS pos, val
        |""".stripMargin).show()

    //test
    ss.sql(
      """
        |SELECT space(months_between('2021-07-20','2020-10-01'))
        |""".stripMargin).show()
  }

}
