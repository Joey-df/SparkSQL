package questions.row_col_transfer

import org.apache.spark.sql.{DataFrame, SparkSession}

object SQL_行列转换5_答案 {

  def main(args: Array[String]): Unit = {


    val ss = SparkSession.builder()
      .master("local")
      .appName("test0")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()
    ss.sparkContext.setLogLevel("ERROR")

    import ss.implicits._

    val df1: DataFrame = List(
      ("产品A","sup_1"),
      ("产品A","sup_2"),
      ("产品A","sup_3"),
      ("产品B","sup_1"),
      ("产品B","sup_2"),
      ("产品B","sup_4"),
      ("产品C","sup_2"),
      ("产品C","sup_3"),
      ("产品C","sup_4")
    ).toDF("Product","Supplier")

    df1.createTempView("tableA")
    ss.sql(
      """
        | select * from tableA
        |""".stripMargin)
      .show()

    //todo
//    //方法1：使用union
//    ss.sql(
//      """
//        |
//        |""".stripMargin)
//      .show()
//
//    //方法2：使用explode函数
//    ss.sql(
//      """
//        |
//        |""".stripMargin)
//      .show()





    val df2: DataFrame = List(
      ("产品A","sup_1,sup_2,sup_3"),
      ("产品B","sup_1,sup_2,sup_4"),
      ("产品C","sup_2,sup_3,sup_4")
    ).toDF("Product","Supplier")

    df2.createTempView("tableB")
    ss.sql(
      """
        | select * from tableB
        |""".stripMargin)
      .show()

    //todo
  }

}
