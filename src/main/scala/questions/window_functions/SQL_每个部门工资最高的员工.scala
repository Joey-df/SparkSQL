package questions.window_functions

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 找出每个部门工资最高的员工。(并列最高需均输出)
 * emp_info表包含所有员工信息，每个员工有其对应的 id, name, salary, deptId
 * id   name    salary    deptId
 * 1    Joe     7000       1
 * 2    Jim     9000       1
 * 3    Sam     8000       2
 * 4    Herry   6000       3
 * 5    Lilei   9000       1
 *
 * dept_info表包含公司所有部门的信息
 * id   name
 * 1    IT
 * 2    SALES
 */
object SQL_每个部门工资最高的员工 {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder()
      .master("local")
      .appName("test")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    ss.sparkContext.setLogLevel("ERROR")

    import ss.implicits._

    val df1: DataFrame = List(
      (1, "Joe", "7000", "1"),
      (2, "Jim", "9000", "1"),
      (3, "Sam", "8000", "2"),
      (4, "Herry", "6000", "3"),
      (5, "Lilei", "9000", "1")
    ).toDF("id", "name", "salary", "deptId")

    df1.createTempView("data")

    ss.sql(
      """
        |select id,deptId,name,salary,rn
        |from (select id,
        |             deptId,
        |             name,
        |             salary,
        |             rank() over(partition by deptId order by salary desc) as rn
        |      from data) t
        |where rn=1
        |""".stripMargin)
      .show()


  }
}
