package questions.practice2

// 处理产品版本号版本号信息存储在数据表中，每行一个版本号。
// 版本号命名规则如下：
// 产品版本号由三个部分组成
// 如：v9.11.2
// 第一部分9为主版本号，为1-99之间的数字；
// 第二部分11为子版本号，为0-99之间的数字；
// 第三部分2为阶段版本号，为0-99之间的数字（可选）；
//
// 已知T1表有若干个版本号：
// version_id (版本号)
// v9.9.2
// v9.0.8
// v9.9.2
// v9.20
// v31.0.10
//
// 请使用hive sql编程实现如下2个小需求：
// 1.需求A：找出T1表中最大的版本号。
// 2.需求B：计算出如下格式的所有版本号排序，要求对于相同的版本号，
// 顺序号并列：
// version_id (版本号)  seq (顺序号)
// v31.0.10               0
// v9.20                  1
// v9.9.2                 2
// v9.9.2                 2
// v9.0.8                 4
// https://blog.csdn.net/Aeve_imp/article/details/105878068
object Problem_处理产品版本号 {

  //todo

}
