package questions.practice1

/**
 * 有一个账号表如下，请写出SQL语句，查询各自区组的money排名前十的账号（分组取前10）
 *
 * CREATE TABIE `account`
 * (
 * `dist_id` int（11）
 * DEFAULT NULL COMMENT '区组id'，
 * `account` varchar（100）DEFAULT NULL COMMENT '账号' ,
 * `gold` int（11）DEFAULT NULL COMMENT '金币'
 * PRIMARY KEY （`dist_id`，`account_id`），
 * ）ENGINE=InnoDB DEFAULT CHARSET-utf8
 */
object Problem_0012 {

  def main(args: Array[String]): Unit = {

  }
}
