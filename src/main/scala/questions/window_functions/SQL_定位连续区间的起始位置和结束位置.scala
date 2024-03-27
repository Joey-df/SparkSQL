package questions.window_functions

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Author: liupei
 * @Description:
 * @Date: 2022/11/14 14:54
 *
 * id 是上表temp_id_list_0209的主键，
 * 表每一行包含日志表中的一个 ID，现将一些 ID 从 Logs 表中删除。
 * 编写一个 SQL 查询得到 Logs 表中的连续区间的开始数字和结束数字，将查询表按照 start_id 排序。
 * 要求输出结果为:
 * start_id  end_id
 * 1         2
 * 4         6
 * 9         12
 */
object SQL_定位连续区间的起始位置和结束位置 {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder()
      .master("local")
      .appName("test")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    ss.sparkContext.setLogLevel("ERROR")

    import ss.implicits._

    val df1: DataFrame = List(
      (1),
      (2),
      (4),
      (5),
      (6),
      (9),
      (10),
      (11),
      (12)
    ).toDF("id")

    df1.createTempView("temp_id_list_0209")

    // 第一步 ：获取ID，借助函数构建差值
    ss.sql(
      """
        |select id,
        |       lag(id, 1, id) over (order by id)      df1,
        |       id - lag(id, 1, id) over (order by id) df2
        |from temp_id_list_0209
        |""".stripMargin).show()

    // 第二步、根据第一步结果，创造分组条件
    ss.sql(
      """
        |select id,
        |       sum(case when dif2 > 1 then 1 else 0 end) over (order by id) flag
        |from (
        |         select id,
        |                id - lag(id, 1, id) over (order by id) dif2
        |         from temp_id_list_0209
        |     ) t0
        |""".stripMargin)
      .show()

    // 第三步、根据flag 分组

    ss.sql(
      """
        |select min(id) start_id,
        |       max(id) end_id
        |from (
        |         select id,
        |                sum(if(diff > 1, 1, 0)) over (order by id) as flag
        |         from (
        |                  select id
        |                       , id - lag(id, 1, id) over (order by id) as diff
        |                  from temp_id_list_0209
        |              ) t0
        |     ) t1
        |group by flag
        |order by 1
        |""".stripMargin)
      .show()
  }
}
