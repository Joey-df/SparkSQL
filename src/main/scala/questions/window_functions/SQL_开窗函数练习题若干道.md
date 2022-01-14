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
