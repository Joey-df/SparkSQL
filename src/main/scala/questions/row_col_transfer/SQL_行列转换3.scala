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
object SQL_行列转换3 {

  def main(args: Array[String]): Unit = {


    val ss = SparkSession.builder()
      .master("local")
      .appName("test0")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()
    ss.sparkContext.setLogLevel("ERROR")

    import ss.implicits._

    val df1: DataFrame = List(
      (10000,"a"),
      (10000,"b"),
      (10000,"c"),
      (20000,"c"),
      (20000,"d")
    ).toDF("qq","game")

    df1.createTempView("tableA")
    ss.sql(
      """
        | select * from tableA
        |""".stripMargin)
      .show()

    //TODO
  }

}
