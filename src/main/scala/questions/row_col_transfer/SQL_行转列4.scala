package questions.row_col_transfer

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 我们假设有4款产品ABCD，分别在三个电商平台天猫、淘宝和京东上进行销售，
 * 下表分别以两种形式记录了某个月各产品（Product）在各个平台（Platform）的销售数量（Quantity）。
 * 我们为了分析的方便，需要对两种形式进行转换，也就是我们常说的行/列转换。
 *
 * 考察函数：
 *  concat
 *  collect_list ,collect_set
 *  concat_ws("分隔符"，list)
 *  str_to_map(str,",",":")
 *  if(exp, true_res, false_res)
 */
object SQL_行转列4 {

  def main(args: Array[String]): Unit = {


    val ss = SparkSession.builder()
      .master("local")
      .appName("test0")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()
    ss.sparkContext.setLogLevel("ERROR")

    import ss.implicits._

    val df1: DataFrame = List(
      ("产品A", "Tamll", 90),
      ("产品B", "Tamll", 81),
      ("产品C", "Tamll", 79),
      ("产品A", "taobao", 85),
      ("产品B", "taobao", 86),
      ("产品C", "taobao", 92),
      ("产品A", "JD", 87),
      ("产品B", "JD", 98),
      ("产品C", "JD", 93)
    ).toDF("Product","Platform","Quantity")


    df1.createTempView("table_source")
    ss.sql(
      """
        | select * from table_source
        |""".stripMargin)
      .show()

    //方法1 str_to_map + concat_ws + collect_list

    //方法2 if 或 case when
  }

}
