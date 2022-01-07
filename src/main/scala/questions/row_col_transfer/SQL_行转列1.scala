package questions.row_col_transfer

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 考察函数：
 *  concat
 *  collect_list ,collect_set
 *  concat_ws("分隔符"，list)
 *  str_to_map(str,",",":")
 *  if(exp, true_res, false_res)
 */
object SQL_行转列1 {

  def main(args: Array[String]): Unit = {


    val ss = SparkSession.builder()
      .master("local")
      .appName("test0")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()
    ss.sparkContext.setLogLevel("ERROR")

    import ss.implicits._

    val df1: DataFrame = List(
      ("张三", "Math", 50),
      ("张三", "Chinese", 60),
      ("张三", "English", 70),
      ("李四", "Math", 55),
      ("李四", "Chinese", 66),
      ("李四", "English", 77),
      ("王五", "Math", 44),
      ("王五", "English", 56)
    ).toDF("name","course","score")

    df1.createTempView("TEST_TB_GRADE")
    ss.sql(
      """
        | select * from TEST_TB_GRADE
        |""".stripMargin)
      .show()

    //方法1
    ss.sql(
      """
        | select
        |   name,
        |   nvl(mp['Math'],0) as Math,
        |   nvl(mp['Chinese'],0) as Chinese,
        |   nvl(mp['English'],0) as English
        |  from
        |     (select
        |      name,
        |      str_to_map( concat_ws(",", collect_list(  concat(course, ":", score)  ))  ) mp
        |      from TEST_TB_GRADE
        |      group by name
        |     ) tmp
        |""".stripMargin)
      .show()

    //方法2
    ss.sql(
      """
        | select
        |   name,
        |   sum(if(course='Math',score,0)) as Math,
        |   sum(if(course='English',score,0)) as English,
        |   sum(if(course='Chinese',score,0)) as Chinese
        | from TEST_TB_GRADE
        | group by name
        |
        |""".stripMargin).show()
  }

}
