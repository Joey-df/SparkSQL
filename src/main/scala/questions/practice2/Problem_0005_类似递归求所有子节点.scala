package questions.practice2

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * SQL：求某ID的所有子结点
 * 给一个表, 有 ID 和 PARENT_ID 两个字段, 然后求某ID的所有子结点
 *
 * ID  PARENT_ID
 * 900   NULL
 * 9011 901
 * 9012 901
 * 9013 9012
 * 9014 9013
 *
 * 求 901 的所有子结点
 * 结果为：
 * 9011
 * 9012
 * 9013
 * 9014
 */
object Problem_0005_类似递归求所有子节点 {

  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder()
      .master("local")
      .appName("test")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    ss.sparkContext.setLogLevel("ERROR")

    import ss.implicits._

    val df1: DataFrame = List(
      ("900","NULL"),
      ("9011","901"),
      ("9012","901"),
      ("9013","9012"),
      ("9014","9013")
    ).toDF("ID","PARENT_ID")

    df1.createTempView("table_info")

    ss.sql(
      """
        | select * from table_info
        |""".stripMargin).show()

    ss.sql(
      """
        |select ID from (
        |    select *
        |    from table_info
        |    where PARENT_ID = '901'
        |
        |    union all
        |
        |    select old.*
        |    from table_info new,
        |         table_info old
        |    where new.ID = old.PARENT_ID
        |)
        |""".stripMargin).show()

  }


}
