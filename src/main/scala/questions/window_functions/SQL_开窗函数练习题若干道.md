# 开窗函数练习题

## 第一题
数据准备：有一张表business，三个字段，分别为name，orderdate，cost
```hql
+----+----------+----+
|name| orderdate|cost|
+----+----------+----+
|jack|2017-01-01|  10|
|tony|2017-01-02|  15|
|jack|2017-02-03|  23|
|tony|2017-01-04|  29|
|jack|2017-01-05|  46|
|jack|2017-04-06|  42|
|tony|2017-01-07|  50|
|jack|2017-01-08|  55|
|mart|2017-04-08|  62|
|mart|2017-04-09|  68|
|neil|2017-05-10|  12|
|mart|2017-04-11|  75|
|neil|2017-06-12|  80|
|mart|2017-04-13|  94|
+----+----------+----+
```
#### 1）查询在2017年4月份购买过的顾客及总人数
① 查询2017年4月份购买的详细信息
```hql
select *
from business
where substring(orderdate, 1, 7) = '2017-04';
结果：
+----+----------+----+
|name| orderdate|cost|
+----+----------+----+
|jack|2017-04-06|  42|
|mart|2017-04-08|  62|
|mart|2017-04-09|  68|
|mart|2017-04-11|  75|
|mart|2017-04-13|  94|
+----+----------+----+
```
② 查询2017年4月份购买的顾客（要求名字不能重复）
```hql
select name
from business
where substring(orderdate, 1, 7) = '2017-04'
group by name;
结果：
+----+
|name|
+----+
|jack|
|mart|
+----+
```
③ 查询2017年4月份，每个顾客购买的次数
```hql
select name,
       count(*) cnt
from business
where substring(orderdate, 1, 7) = '2017-04'
group by name;
结果：
+----+--------+
|name|count(1)|
+----+--------+
|jack|       1|
|mart|       4|
+----+--------+
--sql执行顺序：from、where、group by、count(*)
```
④ 查询在2017年4月份购买过的顾客及总人数
```hql
select name,
       count(*) over
from business
where substring(orderdate, 1, 7) = '2017-04'
group by name;
结果：
+----+------------------------------------------------------------------------+
|name|count(1) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)|
+----+------------------------------------------------------------------------+
|jack|                                                                       2|
|mart|                                                                       2|
+----+------------------------------------------------------------------------+
-- sql的执行顺序：form、where、group by、select、count(*) over() //可以理解为over()是在select之后执行的，最后执行的。
```
③ 和 ④ 的结果差异的原因，主要在于sql执行顺序。  
开窗函数over() 遇到 group by语句，是在group by去重之后的结果集之上，进行开窗的。  

#### 2）查询顾客的购买明细及**月购买**总额
```hql
select name,
       orderdate,
       cost,
       sum(cost) over (partition by month(orderdate) )   monthSum1, --可以理解为group by + sum
       sum(cost) over (partition by month(orderdate) order by orderdate rows between unbounded preceding and unbounded following) monthSum2,
       sum(cost) over (partition by name,month(orderdate) order by orderdate rows between unbounded preceding and unbounded following) userMonthSum
from business;
结果：
+----+----------+----+---------+---------+------------+
|name| orderdate|cost|monthSum1|monthSum2|userMonthSum|
+----+----------+----+---------+---------+------------+
|jack|2017-01-01|  10|    205.0|    205.0|       111.0|
|tony|2017-01-02|  15|    205.0|    205.0|        94.0|
|tony|2017-01-04|  29|    205.0|    205.0|        94.0|
|jack|2017-01-05|  46|    205.0|    205.0|       111.0|
|tony|2017-01-07|  50|    205.0|    205.0|        94.0|
|jack|2017-01-08|  55|    205.0|    205.0|       111.0|
|jack|2017-02-03|  23|     23.0|     23.0|        23.0|
|jack|2017-04-06|  42|    341.0|    341.0|        42.0|
|mart|2017-04-08|  62|    341.0|    341.0|       299.0|
|mart|2017-04-09|  68|    341.0|    341.0|       299.0|
|mart|2017-04-11|  75|    341.0|    341.0|       299.0|
|mart|2017-04-13|  94|    341.0|    341.0|       299.0|
|neil|2017-05-10|  12|     12.0|     12.0|        12.0|
|neil|2017-06-12|  80|     80.0|     80.0|        80.0|
+----+----------+----+---------+---------+------------+
monthSum1、monthSum2结果一样，  
monthSum2：加上了order by子句，表示组内数据累加，其数据的分析范围默认是rows between unbounded preceding and current row，这一点一定要注意。  
userMonthSum是每个用户的月购买总额。  
```
**注意点：**
order by子句会让输入的数据强制排序，order By子句对于诸如row_Number()，Lead()，LAG()等函数是必须的，因为如果数据无序，这些函数的结果就没有任何意义。  
- 如果只使用partition by子句,未指定order by的话,我们的分析函数的作用范围是分组内的数据【**此时相当于group by + 聚合函数函数，如 monthSum1**】.  
- 使用了order by子句,默认数据分析范围是从起点到当前行，往往用来实现累加.  

#### 3）查询顾客的购买明细及到目前为止每个顾客购买总金额
```hql
select name,
       orderdate,
       cost,
       sum(cost) over() as sample1,--所有行相加（整张表作为一个大窗口）
       sum(cost) over(partition by name) as sample2,--按name分组，组内数据相加，类似与group by name + sum(cost)
       sum(cost) over(partition by name order by orderdate) as sample3,--按name分组，组内数据累加
       sum(cost) over(partition by name order by orderdate rows between UNBOUNDED PRECEDING and current row ) as sample4 ,--和sample3一样,由起点到当前行的聚合
       sum(cost) over(partition by name order by orderdate rows between 1 PRECEDING and current row) as sample5, --当前行和前面一行做聚合
       sum(cost) over(partition by name order by orderdate rows between 1 PRECEDING AND 1 FOLLOWING ) as sample6,--当前行和前边一行及后面一行
       sum(cost) over(partition by name order by orderdate rows between current row and UNBOUNDED FOLLOWING ) as sample7 --当前行及后面所有行
from business;
结果：
+----+----------+----+-------+-------+-------+-------+-------+-------+-------+
|name| orderdate|cost|sample1|sample2|sample3|sample4|sample5|sample6|sample7|
+----+----------+----+-------+-------+-------+-------+-------+-------+-------+
|jack|2017-01-01|  10|  661.0|  176.0|   10.0|   10.0|   10.0|   56.0|  176.0|
|jack|2017-01-05|  46|  661.0|  176.0|   56.0|   56.0|   56.0|  111.0|  166.0|
|jack|2017-01-08|  55|  661.0|  176.0|  111.0|  111.0|  101.0|  124.0|  120.0|
|jack|2017-02-03|  23|  661.0|  176.0|  134.0|  134.0|   78.0|  120.0|   65.0|
|jack|2017-04-06|  42|  661.0|  176.0|  176.0|  176.0|   65.0|   65.0|   42.0|
|mart|2017-04-08|  62|  661.0|  299.0|   62.0|   62.0|   62.0|  130.0|  299.0|
|mart|2017-04-09|  68|  661.0|  299.0|  130.0|  130.0|  130.0|  205.0|  237.0|
|mart|2017-04-11|  75|  661.0|  299.0|  205.0|  205.0|  143.0|  237.0|  169.0|
|mart|2017-04-13|  94|  661.0|  299.0|  299.0|  299.0|  169.0|  169.0|   94.0|
|neil|2017-05-10|  12|  661.0|   92.0|   12.0|   12.0|   12.0|   92.0|   92.0|
|neil|2017-06-12|  80|  661.0|   92.0|   92.0|   92.0|   92.0|   92.0|   80.0|
|tony|2017-01-02|  15|  661.0|   94.0|   15.0|   15.0|   15.0|   44.0|   94.0|
|tony|2017-01-04|  29|  661.0|   94.0|   44.0|   44.0|   44.0|   94.0|   79.0|
|tony|2017-01-07|  50|  661.0|   94.0|   94.0|   94.0|   79.0|   79.0|   50.0|
+----+----------+----+-------+-------+-------+-------+-------+-------+-------+
```

#### 4）查询顾客上次的购买时间----lag(x,n)over()和lead(x,n)over()偏移量分析函数的运用
```hql
select name,
       orderdate,
       cost,
       lag(orderdate,1,NULL) over(partition by name order by orderdate) as last_date
from business;
结果：
+----+----------+----+----------+
|name| orderdate|cost| last_date|
+----+----------+----+----------+
|jack|2017-01-01|  10|      null|
|jack|2017-01-05|  46|2017-01-01|
|jack|2017-01-08|  55|2017-01-05|
|jack|2017-02-03|  23|2017-01-08|
|jack|2017-04-06|  42|2017-02-03|
|mart|2017-04-08|  62|      null|
|mart|2017-04-09|  68|2017-04-08|
|mart|2017-04-11|  75|2017-04-09|
|mart|2017-04-13|  94|2017-04-11|
|neil|2017-05-10|  12|      null|
|neil|2017-06-12|  80|2017-05-10|
|tony|2017-01-02|  15|      null|
|tony|2017-01-04|  29|2017-01-02|
|tony|2017-01-07|  50|2017-01-04|
+----+----------+----+----------+
```

#### 5）查询前20%时间的订单信息----ntile(n)
```hql
select *
from (
         select name,
                orderdate,
                cost,
                ntile(5) over (order by orderdate) as bucket
         from business
     ) tmp
where tmp.bucket = 1;
结果：
+----+----------+----+------+
|name| orderdate|cost|bucket|
+----+----------+----+------+
|jack|2017-01-01|  10|     1|
|tony|2017-01-02|  15|     1|
|tony|2017-01-04|  29|     1|
+----+----------+----+------+

select name,
       orderdate,
       cost,
       ntile(3) over (order by cost)                    as bucket3, -- 全局按照cost升序排列,数据切成3份
       ntile(3) over (partition by name order by cost ) as bucket4  -- 按照name分组，在分组内按照cost升序排列,数据切成3份
from business;
结果：
+----+----------+----+-------+-------+
|name| orderdate|cost|bucket3|bucket4|
+----+----------+----+-------+-------+
|jack|2017-01-01|  10|      1|      1|
|neil|2017-05-10|  12|      1|      1|
|tony|2017-01-02|  15|      1|      1|
|jack|2017-02-03|  23|      1|      1|
|tony|2017-01-04|  29|      1|      2|
|jack|2017-04-06|  42|      2|      2|
|jack|2017-01-05|  46|      2|      2|
|tony|2017-01-07|  50|      2|      3|
|jack|2017-01-08|  55|      2|      3|
|mart|2017-04-08|  62|      2|      1|
|mart|2017-04-09|  68|      3|      1|
|mart|2017-04-11|  75|      3|      2|
|neil|2017-06-12|  80|      3|      2|
|mart|2017-04-13|  94|      3|      3|
+----+----------+----+-------+-------+
```

#### 6）求每位顾客当前购买日期与最开始购买日期的时间间隔
```hql
select name,
       orderdate,
       cost,
       datediff(orderdate, fv) diff1,
       datediff(cur_lv, fv)    diff2
from (
         select name,
                orderdate,
                cost,
                first_value(orderdate) over (partition by name order by orderdate) fv,
                last_value(orderdate) over (partition by name order by orderdate)  cur_lv --等于orderdate，因为默认是rows between unbounded preceding and current row
         from business
     ) tmp;
结果：
+----+----------+----+-----+-----+
|name| orderdate|cost|diff1|diff2|
+----+----------+----+-----+-----+
|jack|2017-01-01|  10|    0|    0|
|jack|2017-01-05|  46|    4|    4|
|jack|2017-01-08|  55|    7|    7|
|jack|2017-02-03|  23|   33|   33|
|jack|2017-04-06|  42|   95|   95|
|mart|2017-04-08|  62|    0|    0|
|mart|2017-04-09|  68|    1|    1|
|mart|2017-04-11|  75|    3|    3|
|mart|2017-04-13|  94|    5|    5|
|neil|2017-05-10|  12|    0|    0|
|neil|2017-06-12|  80|   33|   33|
|tony|2017-01-02|  15|    0|    0|
|tony|2017-01-04|  29|    2|    2|
|tony|2017-01-07|  50|    5|    5|
+----+----------+----+-----+-----+
```

## 第二题
数据准备    
有一张表log_info，三个字段logday、userid、score   
```hql
+--------+------+-----+
|  logday|userid|score|
+--------+------+-----+
|20191020| 11111|   85|
|20191020| 22222|   83|
|20191020| 33333|   86|
|20191021| 11111|   87|
|20191021| 22222|   65|
|20191021| 33333|   98|
|20191022| 11111|   67|
|20191022| 22222|   34|
|20191022| 33333|   88|
|20191023| 11111|   99|
|20191023| 22222|   33|
+--------+------+-----+
```
#### 1、使用 over() 函数进行数据统计, 统计每个用户及表中数据的总数

#### 2、求用户明细并统计每天的用户总数

#### 3、计算从第一天到现在的所有 score 大于80分的用户总数

#### 4、计算每个用户到当前日期分数大于80的天数


## 第三题
原始数据(学生成绩信息)表score_info   
三个字段 name、subject、score  
```hql
+----+-------+-----+
|name|subject|score|
+----+-------+-----+
| 孙悟空|     语文|   87|
| 孙悟空|     数学|   95|
| 孙悟空|     英语|   68|
|  大海|     语文|   94|
|  大海|     数学|   56|
|  大海|     英语|   84|
|  宋宋|     语文|   64|
|  宋宋|     数学|   86|
|  宋宋|     英语|   84|
|  婷婷|     语文|   65|
|  婷婷|     数学|   85|
|  婷婷|     英语|   78|
+----+-------+-----+
```
#### 1、每门学科学生成绩排名(是否并列排名、空位排名三种实现)
    
#### 2、每门学科成绩排名top n的学生

## 第四题
出处 https://www.jianshu.com/p/07a410bf5bbf   
假设有一个网店，上线了100多个商品，每个顾客浏览任何一个商品时都会产生一条浏览记录，  
浏览记录存储的表名为product_view，访客的用户id为user_id，浏览的商品名称是product_id。  
```hql
数据：
+-------+----------+
|user_id|product_id|
+-------+----------+
|  A0001| product_a|
|  A0002| product_b|
|  A0001| product_b|
|  A0001| product_a|
|  A0003| product_c|
|  A0004| product_b|
|  A0001| product_a|
|  A0002| product_c|
|  A0005| product_b|
|  A0004| product_b|
|  A0006| product_c|
|  A0002| product_c|
|  A0001| product_b|
|  A0002| product_a|
|  A0002| product_a|
|  A0003| product_a|
|  A0005| product_a|
|  A0005| product_a|
|  A0005| product_a|
+-------+----------+
需求：每个商品浏览次数top3 的 用户信息，输出商品id、用户id、浏览次数。  
```
##### 思路：
第1步：计算每个商品被每个用户浏览的次数    
第2步：每个商品被浏览次数排名  
第3步：计算每个商品浏览前3的用户   
```hql

```


## 第五题 计算连续得分的球员
出处 https://www.jianshu.com/p/07a410bf5bbf  
现有一张表 score_info，记录了一场篮球比赛中各个球员的得分记录，即某个球员user_id得分了，就记录该球员的得分时间score_time和得分score。  
需求：计算连续3次得分的用户数，而且中间不能有别的球员得分。
```hql
数据：
+-------+----------+-----+
|user_id|score_time|score|
+-------+----------+-----+
|  A0001|        t1|    2|
|  A0001|        t2|    2|
|  A0002|        t3|    3|
|  A0001|        t4|    2|
|  A0001|        t5|    2|
|  A0001|        t6|    2|
|  A0003|        t7|    3|
|  A0003|        t8|    2|
+-------+----------+-----+
```
思路：   
本题的难点在于需要构造一个可以相减的字段字段，这也是与3天连续登录问题的区别之处。
```hql

```

## 第六题 用户抽奖行为分析
>出处   
https://www.jianshu.com/p/c2ef5e3cb590    
https://blog.csdn.net/licent2011/article/details/121370208    
  
业务背景      
某购物APP最近上线了一个新功能，用户签到后可以跳转到大转盘抽奖，抽奖获得的奖金可以抵消购物的费用，以此来培养用户使用app的习惯。  
    
数据表介绍
现有一张用户行为表user_log，记录了用户在app上的所有行为日志，      
主要字段如下，即何人user_id在何时event_time进行了什么操作event_id。  
```hql
+-------+-------------------+--------+
|user_id|         event_time|event_id|
+-------+-------------------+--------+
|  10001|2021-01-01 07:12:05|   login|
|  10001|2021-01-01 07:13:47|register|
|  10001|2021-01-02 09:46:26|   login|
|  10001|2021-01-02 09:47:15|register|
|  10001|2021-01-02 09:47:25|    gift|
|  10001|2021-01-02 09:52:01|  logout|
|  10002|2021-01-03 18:53:02|   login|
|  10002|2021-01-03 18:54:10|    view|
|  10002|2021-01-03 18:56:34|    cart|
|  10002|2021-01-03 18:58:23|   check|
+-------+-------------------+--------+
```    
需求：      
1、计算该app每天的访客数、以及每天人均行为次数。    
```hql

```  
2、统计每天签到之后并进行抽奖的用户数，注意签到和抽奖行为必须相邻（签到和抽奖行为对应的event_id分别为'register','gift'）。 
```hql

```           


## 第七题 活动效果数据分析
>出处   
https://www.jianshu.com/p/c2ef5e3cb590    
https://blog.csdn.net/licent2011/article/details/121370208   

业务背景   
为了提高某店铺的营业额，最近运营部门上线了多个运营活动，用户参与活动后可以领取不定额的优惠券，以此鼓励用户下单。但每个用户限参与一个活动，不可同时参与多个活动。   
数据表介绍   
现有一张订单表orders和活动报名表act_join,分别记录了用户的下单明细和用户报名的活动明细。具体字段如下：   
订单表orders，大概字段有（user_id '用户编号',order_id '订单编号' ，order_sales '订单金额', order_time '下单时间'）。  
```hql
+-------+---------+-----------+-------------------+
|user_id| order_id|order_sales|         order_time|
+-------+---------+-----------+-------------------+
|  10001|ord_13361|        120|2021-07-01 07:12:05|
|  10002|ord_13247|        100|2021-07-03 07:13:47|
|  10003|ord_10924|        150|2021-07-05 09:46:26|
|  10005|ord_13071|        250|2021-07-04 09:47:15|
|  10006|ord_14533|        450|2021-07-08 09:47:25|
|  10007|ord_11134|        140|2021-07-12 09:52:01|
|  10008|ord_14305|        180|2021-07-09 18:53:02|
|  10009|ord_13055|        220|2021-07-14 18:54:10|
|  10011|ord_10460|        190|2021-07-10 18:56:34|
|  10012|ord_11959|        210|2021-07-13 18:58:23|
+-------+---------+-----------+-------------------+
```    
活动报名表act_join，大概字段有（act_id '活动编号', user_id '报名用户', join_time '报名时间'）      
```hql
+------+-------+-------------------+
|act_id|user_id|          join_time|
+------+-------+-------------------+
|act_01|  10001|2021-07-01 07:12:05|
|act_01|  10002|2021-07-03 07:13:47|
|act_01|  10003|2021-07-05 09:46:26|
|act_01|  10005|2021-07-04 09:47:15|
|act_02|  10006|2021-07-08 09:47:25|
|act_02|  10007|2021-07-12 09:52:01|
|act_02|  10008|2021-07-09 18:53:02|
|act_03|  10009|2021-07-14 18:54:10|
|act_03|  10011|2021-07-10 18:56:34|
|act_03|  10012|2021-07-13 18:58:23|
+------+-------+-------------------+
```
需求：      
1、统计每个活动报名的所有用户在报名后产生的总订单金额，总订单数。（每个用户限报一个活动,且默认用户报名后产生的订单均为参加活动的订单）。      
```hql

```
2、统计每个活动从开始后到当天（数据统计日）平均每天产生的订单数，活动开始时间定义为最早有用户报名的时间。      
```hql

```