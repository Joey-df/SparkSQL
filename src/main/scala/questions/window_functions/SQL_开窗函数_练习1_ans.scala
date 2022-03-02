package questions.window_functions

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 建表 & 导入数据
 *
 * create table sale_info (
 * date datetime,
 * value int
 * );
 * insert into sale_info values
 * ("2018/11/23",10),
 * ("2018/12/31",3),
 * ("2019/2/9",53),
 * ("2019/3/31",23),
 * ("2019/7/8",11),
 * ("2019/7/31",10);
 */
object SQL_开窗函数_练习1_ans {

  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder()
      .master("local")
      .appName("test")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    ss.sparkContext.setLogLevel("ERROR")

    import ss.implicits._

    val df1: DataFrame = List(
      ("2018/11/23",10),
      ("2018/12/31",3),
      ("2019/2/9",53),
      ("2019/3/31",23),
      ("2019/7/8",11),
      ("2019/7/31",10)
    ).toDF("fdate","value")

    df1.createTempView("sale_info")
    ss.sql(
      """
        | select * from sale_info
        |""".stripMargin)
      .show()

    //第一问
    //-- 添加索引 给 date列;
    //create index id_date on sale_info(date);
    //show index from sale_info;


    //第二问
    ss.sql(
      """
        |select fyear, fmonth, value,
        |       sum(value) over(partition by fyear order by fmonth) as ysum,
        |       sum(value) over(rows between unbounded preceding and current row) as sum
        |from (
        |         select fyear, fmonth, sum(val) as value
        |         from (
        |                  select year(regexp_replace(fdate, '/', '-'))  fyear,
        |                         month(regexp_replace(fdate, '/', '-')) fmonth,
        |                         value as                               val
        |                  from sale_info
        |                  order by fyear, fmonth
        |              ) t0
        |         group by fyear, fmonth
        |     ) t1
        |""".stripMargin)
      .show()


    //第二问的另一种方法
    ss.sql(
      """
        |select fyear, fmonth, value,
        |       sum(value) over(partition by fyear order by fmonth) as ysum,
        |       sum(value) over(order by concat(fyear,'-',fmonth)) as sum  -- 使用了order by子句，默认数据分析范围是从起点到当前行，往往用来实现累加.
        |from (
        |         select fyear, fmonth, sum(val) as value
        |         from (
        |                  select year(regexp_replace(fdate, '/', '-'))  fyear,
        |                         month(regexp_replace(fdate, '/', '-')) fmonth,
        |                         value as                               val
        |                  from sale_info
        |                  order by fyear, fmonth
        |              ) t0
        |         group by fyear, fmonth
        |     ) t1
        |""".stripMargin)
      .show()
  }
}
