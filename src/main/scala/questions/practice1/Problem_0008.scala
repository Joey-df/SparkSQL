package questions.practice1

/**
 * 第八题（尚硅谷第9题）
 * 需求：
 * 充值日志表credit_log，字段如下：
 * CREATE TABLE `credit log`
 * (
 * `dist_id` int（11）DEFAULT NULL COMMENT '区组id',
 * `account` varchar（100）DEFAULT NULL COMMENT '账号',
 * `money` int(11) DEFAULT NULL COMMENT '充值金额',
 * `create_time` datetime DEFAULT NULL COMMENT '订单时间'
 * ) ENGINE=InnoDB DEFAUILT CHARSET-utf8
 *
 * 请写出SQL语句，查询充值日志表2020年08月08号每个区组下、充值额最大的账号，要求结果：
 * 区组id，账号，金额，充值时间
 */
object Problem_0008 {

  def main(args: Array[String]): Unit = {

    // SELECT t1.dist_id,
    //        t1.account,
    //        t1.sum_money
    // FROM (
    //          SELECT temp.dist_id,
    //                 temp.account,
    //                 temp.sum_money,
    //                 rank() over (partition BY temp.dist_id ORDER BY temp.sum_money desc) rk
    //          FROM (
    //                   SELECT dist_id, -- 区组id
    //                          account, -- 账号id
    //                          sum(money) sum_money
    //                   FROM test_db.credit_log
    //                   WHERE date_format(create_time, 'yyyy-MM-dd') = '2020-08-08'
    //                   GROUP BY dist_id, account
    //               ) temp -- 先按区组id+账号id 统计 出总充值金额
    //      ) t1
    // WHERE rk = 1;

  }

}
