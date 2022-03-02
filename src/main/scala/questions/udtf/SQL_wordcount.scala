package questions.udtf

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 使用sql实现word count
 *
 * 现有文件words.txt 内容如下：
 * Spark Hive Hdfs Spark
 * Hdfs Spark Hive Yarn
 * 统计该文件每个单词出现的频率（按照词频降序输出）：
 * 结果：
 * Spark 3
 * Hdfs 2
 * Hive 2
 * Yarn 1
 */
object SQL_wordcount {

  //先创建一个只有一列的表word_info，列名为word：
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder()
      .master("local")
      .appName("test0")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()
    ss.sparkContext.setLogLevel("ERROR")

    import ss.implicits._

    val df: DataFrame = List(
      ("Spark Hive Hdfs Spark"),
      ("Hdfs Spark Hive Yarn Hive")
    ).toDF("line")

    df.createTempView("words")

    ss.sql(
      """
        | select * from words
        |""".stripMargin)
      .show()

    ss.sql(
      """
        |select word, count(1) as cnt
        |from (
        |         select word from words
        |         lateral view outer explode(split(line, ' ')) xx as word
        |     ) tmp
        |group by word
        |order by cnt desc
        |""".stripMargin)
      .show()
  }
}
