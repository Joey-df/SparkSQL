package questions.practice1

/**
 * 第七题
 * 需求：
 * 有一个账号表table_info如下，请写出SQL语句，查询各自 区组 的money排名前3的账号
 * 字段如下：
 * dist_id string  '区组id',
 * account string  '账号',
 * gold_coin  int  '金币'
 */
object Problem_0007 {

  def main(args: Array[String]): Unit = {

    // SELECT dist_id,
    //        account,
    //        gold_coin
    // FROM (
    //          SELECT dist_id,
    //                 account,
    //                 gold_coin,
    //                 row_number() over (PARTITION BY dist_id ORDER BY gold_coin DESC) rank
    //          FROM test_db.table_info
    //      ) t
    // WHERE rank <= 3;

  }

}
