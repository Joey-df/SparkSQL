package questions.practice2

import org.apache.spark.sql.SparkSession

object SQL_Test_parse_url {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder()
      .master("local")
      .appName("test0")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()
    ss.sparkContext.setLogLevel("ERROR")

    //以下测试parse_url
    ss.sql(
      """
        |select parse_url('https://www.baidu.com/path1/p.php?k1=v1&k2=v2#Ref1', 'PATH')
        |""".stripMargin).show()

    ss.sql(
      """
        |select parse_url('https://www.baidu.com/path1/p.php?k1=v1&k2=v2#Ref1', 'QUERY')
        |""".stripMargin).show()

    ss.sql(
      """
        |select parse_url('https://www.baidu.com/path1/p.php?k1=v1&k2=v2#Ref1', 'QUERY', 'k1')
        |""".stripMargin).show()

    ss.sql(
      """
        |select parse_url('https://www.baidu.com/path1/p.php?k1=v1&k2=v2#Ref1', 'FILE')
        |""".stripMargin).show()
  }

}
