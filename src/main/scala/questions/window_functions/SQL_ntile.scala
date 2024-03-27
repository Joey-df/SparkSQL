package questions.window_functions

import org.apache.spark.sql.{DataFrame, SparkSession}

object SQL_ntile {

  def main(args: Array[String]): Unit = {
    //+-------+-------+---------+------+----------+
    //|name   |dept_no|employ_id|salary|entry_time|
    //+-------+-------+---------+------+----------+
    //|mike   |1      |1        |10000 |2014-01-29|
    //|tom    |1      |2        |8000  |2013-10-02|
    //|john   |1      |3        |6000  |2014-10-02|
    //|jerry  |2      |4        |6600  |2012-11-03|
    //|jack   |2      |5        |5000  |2010-01-03|
    //|rose   |2      |6        |4000  |2014-11-29|
    //|steven |3      |7        |5000  |2014-12-02|
    //|richard|3      |8        |9000  |2013-11-03|
    //+-------+-------+---------+------+----------+
    val ss = SparkSession.builder()
      .master("local")
      .appName("test")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    ss.sparkContext.setLogLevel("ERROR")

    import ss.implicits._

    val df1: DataFrame = List(
      ("mike",1,1,10000,"2014-01-29"),
      ("tom",1,2,8000,"2013-10-02"),
      ("john",1,3,6000,"2014-10-02"),
      ("jerry",2,4,6600,"2012-11-03"),
      ("jack",2,5,5000,"2010-01-03"),
      ("rose",2,6,4000,"2014-11-29"),
      ("steven",3,7,5000,"2014-12-02"),
      ("richard",3,8,9000,"2013-11-03")
    ).toDF("name","dept_no","employ_id","salary","entry_time")

    df1.createTempView("data")

    // ntile
    ss.sql(
      """
        |SELECT name, dept_no, salary,
        |       ntile(2) over(order by salary) n1,-- 全局按照salary升序排列，数据切成2份
        |       ntile(2) over(partition by dept_no order by salary) n2, -- 按照dept_no分组，在分组内按照salary升序排列,数据切成2份
        |       ntile(3) over(partition by dept_no order by salary) n3 -- 按照dept_no分组，在分组内按照salary升序排列,数据切成3份
        |FROM data
        |""".stripMargin)
      .show()

    // fisrt_val
    ss.sql(
    """
      |select name,
      |       dept_no,
      |       salary,
      |       first_value(salary) over(partition by dept_no order by salary asc) as fv
      |from data
      |""".stripMargin
    ).show()

    //last_value
    ss.sql(
      """
        |select name,
        |       dept_no,
        |       salary,
        |       last_value(salary) over(partition by dept_no order by salary asc rows between unbounded preceding and unbounded following) as fv
        |from data
        |""".stripMargin
    ).show()

    //lead lag
    ss.sql(
      """
        |select name,
        |       dept_no,
        |       salary,
        |       lead(salary, 1, -1) over(partition by dept_no order by salary asc) as lead,
        |       lag(salary, 1, -2) over(partition by dept_no order by salary asc) as lag
        |from data
        |""".stripMargin
    ).show()

  }
}
