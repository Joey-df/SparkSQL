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
--sql执行顺序：from、where、group by、count(*)、select
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
       sum(cost) over (partition by name, month(orderdate) order by orderdate rows between unbounded preceding and unbounded following) userMonthSum
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
**order by子句会让输入的数据强制排序，order by子句对于诸如row_number()，Lead()，LAG()等函数是必须的，因为如果数据无序，这些函数的结果就没有任何意义**。  
- 如果只使用partition by子句，未指定order by的话，我们的分析函数的作用范围是分组内的数据【**此时相当于group by + 聚合函数函数，如 monthSum1**】。     
- 使用了order by子句，默认数据分析范围是从起点到当前行，往往用来实现累加.  

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

#### 4）查询顾客上次的购买时间----lag(x,n) over()和lead(x,n) over() 偏移量分析函数的运用
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
```hql
select userid,
       logday,
       score,
       count(*) over ()                    count_all,     --表中数据总数
       count(*) over (partition by userid) count_cur_user --每个用户数据总数
from log_info;
结果：
+------+--------+-----+---------+--------------+
|userid|  logday|score|count_all|count_cur_user|
+------+--------+-----+---------+--------------+
| 11111|20191020|   85|       11|             4|
| 11111|20191021|   87|       11|             4|
| 11111|20191022|   67|       11|             4|
| 11111|20191023|   99|       11|             4|
| 22222|20191020|   83|       11|             4|
| 22222|20191021|   65|       11|             4|
| 22222|20191022|   34|       11|             4|
| 22222|20191023|   33|       11|             4|
| 33333|20191020|   86|       11|             3|
| 33333|20191021|   98|       11|             3|
| 33333|20191022|   88|       11|             3|
+------+--------+-----+---------+--------------+

```
#### 2、求用户明细并统计每天的用户总数
```hql
select userid,
       logday,
       score,
       count(*) over (partition by logday) count_cur_day
from log_info;
结果：
+------+--------+-----+-------------+
|userid|  logday|score|count_cur_day|
+------+--------+-----+-------------+
| 11111|20191020|   85|            3|
| 22222|20191020|   83|            3|
| 33333|20191020|   86|            3|
| 11111|20191021|   87|            3|
| 22222|20191021|   65|            3|
| 33333|20191021|   98|            3|
| 11111|20191022|   67|            3|
| 22222|20191022|   34|            3|
| 33333|20191022|   88|            3|
| 11111|20191023|   99|            2|
| 22222|20191023|   33|            2|
+------+--------+-----+-------------+
```
#### 3、计算从第一天到现在的 所有 score 大于80分的记录总数
```hql
select *,
       count(1) over (order by logday)                                                    cur_total1,
       count(1) over (order by logday rows between unbounded preceding and current row) as cur_total2
from log_info
where score > 80;
结果：
+--------+------+-----+----------+----------+
|  logday|userid|score|cur_total1|cur_total2|
+--------+------+-----+----------+----------+
|20191020| 11111|   85|         3|         1|
|20191020| 22222|   83|         3|         2|
|20191020| 33333|   86|         3|         3|
|20191021| 11111|   87|         5|         4|
|20191021| 33333|   98|         5|         5|
|20191022| 33333|   88|         6|         6|
|20191023| 11111|   99|         7|         7|
+--------+------+-----+----------+----------+
注意 cur_total1 和 cur_total2 的区别！！！
```
#### 4、计算每个用户到当前日期分数大于80的 天数（记录数）
```hql
select userid,
       logday,
       score,
       count(1) over (partition by userid order by logday asc)                                                   cur_cnt1,
       count(1) over (partition by userid order by logday asc rows between unbounded preceding and current row ) cur_cnt2
from log_info
where score > 80;
结果：
+------+--------+-----+--------+--------+
|userid|  logday|score|cur_cnt1|cur_cnt2|
+------+--------+-----+--------+--------+
| 11111|20191020|   85|       1|       1|
| 11111|20191021|   87|       2|       2|
| 11111|20191023|   99|       3|       3|
| 22222|20191020|   83|       1|       1|
| 33333|20191020|   86|       1|       1|
| 33333|20191021|   98|       2|       2|
| 33333|20191022|   88|       3|       3|
+------+--------+-----+--------+--------+
```


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
```hql
select name,
       subject,
       score,
       row_number() over (partition by subject order by score desc) as rn,
       rank() over (partition by subject order by score desc)       as rank,
       dense_rank() over (partition by subject order by score desc) as d_rank
from score_info;
结果：
+----+-------+-----+---+----+------+
|name|subject|score| rn|rank|d_rank|
+----+-------+-----+---+----+------+
| 孙悟空|     数学|   95|  1|   1|     1|
|  宋宋|     数学|   86|  2|   2|     2|
|  婷婷|     数学|   85|  3|   3|     3|
|  大海|     数学|   56|  4|   4|     4|
|  大海|     英语|   84|  1|   1|     1|
|  宋宋|     英语|   84|  2|   1|     1|
|  婷婷|     英语|   78|  3|   3|     2|
| 孙悟空|     英语|   68|  4|   4|     3|
|  大海|     语文|   94|  1|   1|     1|
| 孙悟空|     语文|   87|  2|   2|     2|
|  婷婷|     语文|   65|  3|   3|     3|
|  宋宋|     语文|   64|  4|   4|     4|
+----+-------+-----+---+----+------+
```    
#### 2、每门学科成绩排名top 3的学生
```hql
select name,
       subject,
       score,
       rn,
       rank,
       d_rank
from (
         select name,
                subject,
                score,
                row_number() over (partition by subject order by score desc) as rn,
                rank() over (partition by subject order by score desc)       as rank,
                dense_rank() over (partition by subject order by score desc) as d_rank
         from score_info
     ) tmp
where rank <= 3; -- 或者rn<=3
结果：
+----+-------+-----+---+----+------+
|name|subject|score| rn|rank|d_rank|
+----+-------+-----+---+----+------+
| 孙悟空|     数学|   95|  1|   1|     1|
|  宋宋|     数学|   86|  2|   2|     2|
|  婷婷|     数学|   85|  3|   3|     3|
|  大海|     英语|   84|  1|   1|     1|
|  宋宋|     英语|   84|  2|   1|     1|
|  婷婷|     英语|   78|  3|   3|     2|
|  大海|     语文|   94|  1|   1|     1|
| 孙悟空|     语文|   87|  2|   2|     2|
|  婷婷|     语文|   65|  3|   3|     3|
+----+-------+-----+---+----+------+
```

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
第1步：计算每个商品 被 每个用户 浏览的次数    
第2步：每个商品被浏览次数排名    
第3步：计算每个商品浏览前3的用户     
```hql
select *
from (
         select user_id,
                product_id,
                view_cnt,
                rank() over (partition by product_id order by view_cnt desc) rn
         from (
                  select user_id,
                         product_id,
                         count(1) view_cnt
                  from product_view
                  group by user_id, product_id
              ) t1 -- 每个商品 被 每个用户 浏览的次数  信息表
     ) t2 -- 每个商品被浏览次数排名  
where rn <= 3;
结果：
+-------+----------+--------+---+
|user_id|product_id|view_cnt| rn|
+-------+----------+--------+---+
|  A0001| product_a|       3|  1|
|  A0005| product_a|       3|  2|
|  A0002| product_a|       2|  3|
|  A0001| product_b|       2|  1|
|  A0004| product_b|       2|  2|
|  A0002| product_b|       1|  3|
|  A0002| product_c|       2|  1|
|  A0003| product_c|       1|  2|
|  A0006| product_c|       1|  3|
+-------+----------+--------+---+
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
计算app每天的访客数，因为用户登录访问app就会在表中产生对应的行为日志，所以每天的访客数只需要按天对用户数去重即可，
每天人均行为次数的计算，因为一次行为就会产生一条记录，所以，人均行为次数就是所有的记录计数，除以总的访客数。
select substring(event_time, 1, 10) as           date,
       count(distinct user_id)                   user_cnt,   -- 每天的访客数
       count(event_id) / count(distinct user_id) avg_opr_cnt -- 每天人均行为次数
from user_log
group by substring(event_time, 1, 10);

或者：
select cast(event_time as date)                  as day,
       count(distinct user_id)                   as active_cnt,
       count(event_id) / count(distinct user_id) as avg_opr_cnt
from user_log
group by cast(event_time as date);

结果：
+----------+--------+-----------+
|      date|user_cnt|avg_opr_cnt|
+----------+--------+-----------+
|2021-01-01|       1|        2.0|
|2021-01-02|       1|        4.0|
|2021-01-03|       1|        4.0|
+----------+--------+-----------+
```  
2、统计每天签到之后并进行抽奖的用户数，注意签到和抽奖行为必须相邻（签到和抽奖行为对应的event_id分别为'register','gift'）。 
```hql
虽然也是统计用户数，但是添加了限制：签到之后要大转盘抽奖，两个行为一前一后必须相邻才可以。
这个时候我们可以用窗口函数的位移函数lead（）over（）实现，lead可以取当前记录的下一条记录，
如果我们对每个用户user_id分组，按照行为时间event_time升序排列，就可以得到一个用户的连续的行为记录，
再用lead（）就可以得到下一条记录，从而在当前记录中得到下一条记录，对两个连续行为进行筛选，就可以计算满足这个条件的用户数。
select date,
       count(distinct user_id)
from (
         select user_id,
                substring(event_time, 1, 10) as                                           date,
                event_time,
                event_id,
                lead(event_id, 1, null)
                     over (partition by substring(event_time, 1, 10) order by event_time) next_event_id
         from user_log
     ) tmp
where event_id = 'register' and next_event_id = 'gift'
group by date;

结果：
+----------+-----------------------+
|      date|count(DISTINCT user_id)|
+----------+-----------------------+
|2021-01-02|                      1|
+----------+-----------------------+
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
思路：
计算总订单金额和总订单数，这两个指标都比较简单sum(order_sales)、count(order_id)就可以，
但是关键在于限定条件，是每个活动报名后的用户的汇总，必须是报名了某个活动，且必须在活动开始后的数据统计。
可以通过订单表orders和报名表act_join连接，限定订单时间大于等于活动的开始时间即可。
select act_id,
       sum(order_sales) sum_sales,
       count(order_id)  order_cnt
from (
         select act_id,
                t1.user_id,
                join_time,
                order_id,
                order_sales,
                order_time
         from act_join t1 inner join orders t2 on t1.user_id = t2.user_id
         where t2.order_time >= t1.join_time
     ) t0
group by act_id;

结果：
+------+---------+---------+
|act_id|sum_sales|order_cnt|
+------+---------+---------+
|act_01|      620|        4|
|act_02|      770|        3|
|act_03|      620|        3|
+------+---------+---------+
```
2、统计每个活动从开始后到当天（数据统计日）平均每天产生的订单数，活动开始时间定义为最早有用户报名的时间。      
```hql
思路：
与第1问有相似之处，同样是用户报名后的下单，
只是多了一些限定条件：同时要满足要小于等于计算日期当天，也就是程序运行的系统时间now()，
在此基础上，计算整体的订单数，除以活动进行的天数，就是该活动每天的平均下单数。
select t1.act_id,
       count(order_id) / datediff(now(), min(t1.begin_time)) --总订单数/活动天数
from (
         select act_id,
                user_id,
                join_time,
                min(join_time) over (partition by act_id) as begin_time --当前活动的开始时间(最早有用户报名的时间)
         from act_join
     ) t1
         inner join
     (
         select user_id,
                order_id,
                order_time
         from orders
     ) t2
     on t1.user_id = t2.user_id
where t1.join_time between t1.begin_time and now() --活动开始至今的数据
  and t2.order_time >= t1.join_time                --活动开始后的下单
group by t1.act_id;

结果：
+------+------------------------------------------------------------------------------------------------------------------------------+
|act_id|(CAST(count(order_id) AS DOUBLE) / CAST(datediff(CAST(current_timestamp() AS DATE), CAST(min(begin_time) AS DATE)) AS DOUBLE))|
+------+------------------------------------------------------------------------------------------------------------------------------+
|act_01|                                                                                                           0.01639344262295082|
|act_02|                                                                                                          0.012658227848101266|
|act_03|                                                                                                           0.01276595744680851|
+------+------------------------------------------------------------------------------------------------------------------------------+
```

## 第八题 计算用户累计成单达到一定金额的时间
订单表：order_t
字段：
orderid
userid
paidtime
paidfee
请使用hive sql计算用户累计成单达到一定金额的时间
输出：userid, keypoint, keypoint_time
keypoint：>1000, >2000, >5000
```hql
源表：
+-------+------+----------+-------+
|orderid|userid|  paidtime|paidfee|
+-------+------+----------+-------+
|      1|     a|2021-04-05|    200|
|      2|     a|2021-04-06|    200|
|      3|     a|2021-04-07|    100|
|      4|     a|2021-04-08|    400|
|      4|     a|2021-04-09|    200|
|      4|     a|2021-04-10|    800|
|      5|     a|2021-04-11|    300|
|      6|     a|2021-04-12|   3000|
|      6|     a|2021-04-13|   4000|
+-------+------+----------+-------+
目标表：
+------+--------+-------------+
|userid|keypoint|keypoint_time|
+------+--------+-------------+
|     a|   >1000|   2021-04-09|
|     a|   >2000|   2021-04-11|
|     a|   >5000|   2021-04-12|
+------+--------+-------------+
```

```hql
select userid,
       keypoint,
       min(paidtime) keypoint_time
from (
         select userid,
                orderid,
                paidtime,
                (case
                     when cur_sum >= 1000 and cur_sum < 2000 then '>1000'
                     when cur_sum >= 2000 and cur_sum < 5000 then '>2000'
                     else '>5000' end) as keypoint
         from (
                  select userid,
                         orderid,
                         paidtime,
                         sum(paidfee) over (partition by userid order by paidtime asc) cur_sum
                  from order_t
              ) tmp
         where cur_sum >= 1000
     ) tt
where keypoint in ('>1000', '>2000', '>5000')
group by userid, keypoint
```