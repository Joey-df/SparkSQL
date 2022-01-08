package questions.practice

/**
 * 第六题（尚硅谷第6题）
 * 需求：
 * 请用sql写出所有用户中在2020年10月份第一次购买商品的金额，
 * 表order字段如下:
 * 购买用户：user_id，金额：money，购买时间：pay_time(格式：2017-10-01)，订单id：order_id
 */
object Problem_0006 {

  def main(args: Array[String]): Unit = {


    // SELECT user_id,
    //        pay_time,
    //        money,
    //        order_id
    // FROM (
    //          SELECT user_id,
    //                 money,
    //                 pay_time,
    //                 order_id,
    //                 row_number() over (PARTITION BY user_id ORDER BY pay_time) rank
    //          FROM test_db.order
    //          WHERE date_format(pay_time, 'yyyy-MM') = '2020-10'
    //      ) t
    // WHERE rank = 1;

  }

}
