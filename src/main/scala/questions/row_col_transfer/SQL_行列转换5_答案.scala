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
    ).toDF("product","supplier")

    df1.createTempView("tableA")
    ss.sql(
      """
        | select * from tableA
        |""".stripMargin)
      .show()

    // udaf
    ss.sql(
      """
        |select product,
        |       concat_ws(',', collect_list(supplier)) as suppliers
        |from tableA
        |group by product
        |""".stripMargin).show()




    val df2: DataFrame = List(
      ("产品A","sup_1,sup_2,sup_3"),
      ("产品B","sup_1,sup_2,sup_4"),
      ("产品C","sup_2,sup_3,sup_4")
    ).toDF("product","suppliers")

    df2.createTempView("tableB")
    ss.sql(
      """
        | select * from tableB
        |""".stripMargin)
      .show()

    //udtf
    ss.sql(
      """
        |select product,
        |       supplier
        |from tableB
        |lateral view explode(split(suppliers, ',')) tf as supplier
        |""".stripMargin).show()
  }

}
