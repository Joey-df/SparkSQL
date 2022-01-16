# 开窗函数 over()

官方文档：
```html
https://cwiki.apache.org/confluence/display/Hive/LanguageManual+WindowingAndAnalytics
```
窗口函数又叫OLAP函数/分析函数，  
over() 才是窗口函数，而sum、row_number、count等只是与over()搭配的分析函数，当然除了这三个函数还有其他的函数。  
>over()窗口函数的语法结构如下        
分析函数 over(partition by 列名 order by 列名 rows between 开始位置 and 结束位置)

>我们在使用over()窗口函数时，over()函数中的这三个函数可组合使用、也可以不使用。    
over()函数中如果不使用这三个函数，窗口大小是针对查询产生的所有数据；     
如果指定了分区，窗口大小是针对每个分区的数据。

与聚合函数类似，开窗函数也是对行集组进行聚合计算。  
但是它不像普通聚合函数那样，每组通常只返回一个值，开窗函数可以为每组返回多个值，因为开窗函数所执行聚合计算的行集组是窗口。  

常见的格式如下：  
```hql
FUNCTION_NAME([argument_list])
OVER (
  [PARTITION BY 分区(分组)列, ...]
  [ORDER BY 排序字段, ... [ASC|DESC]])
  [ { ROWS | RANGE } BETWEEN 开始位置 AND 结束位置 ] 
);

FUNCTION_NAME：分析函数的名称。如row_number()、sum()、first_value()等。  
argument_list：分析函数的参数列表。  
PARTITION BY：partition by可理解为group by 分组。over(partition by 列名)搭配分析函数时，分析函数按照每一组每一组的数据进行计算的。
ORDER BY：将每个partition by分组内的数据（即窗口），根据window_ordering（排序字段）进行排序。  
    ORDER BY子句会对输入的数据强制排序（~~窗口函数是SQL语句最后执行的函数~~，因此可以把SQL结果集想象成输入数据）。  
    ORDER BY子句对于诸如row_number()，lead()，lag()等函数是必须的。如果数据无序，这些函数的结果就没有意义。  
ROWS BETWEEN 开始位置 AND 结束位置：   
窗口范围说明：   
我们常使用的窗口范围是ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW（表示从起点到当前行），常用该窗口来计算累加。 
```
在介绍具体的开窗函数和示例之前，再来了解一下window子句：  

PRECEDING：往前  
FOLLOWING：往后   
CURRENT ROW：当前行  
UNBOUNDED：起点（一般结合PRECEDING，FOLLOWING使用）  
UNBOUNDED PRECEDING 表示该窗口最前面的行（起点）  
UNBOUNDED FOLLOWING：表示该窗口最后面的行（终点）  
> 比如说：   
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW（表示从起点到当前行）      
ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING（表示往前2行到往后1行）      
ROWS BETWEEN 2 PRECEDING AND 1 CURRENT ROW（表示往前2行到当前行）      
ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING（表示当前行到终点）      

后面会有具体应用的示例。  

## 常见聚合函数 + over()
hive支持 count(),max(),min(),sum(),avg() 等常用的聚合函数
>注意：  
 聚合操作时要注意null值    
 count(*) 包含null值，统计所有行数    
 count(id) 不包含null值    
 min 求最小值是不包含null，除非所有值都是null    
 avg 求平均值也是不包含null    


## sum() over() 
数据：  
```hql
+---+----------------+---+
|id |date            |pv |
+---+----------------+---+
|1  |2015-04-10      |1  |
|1  |2015-04-11      |5  |
|1  |2015-04-12      |7  |
|2  |2015-04-13      |3  |
|2  |2015-04-14      |2  |
|2  |2015-04-15      |4  |
|3  |2015-04-16      |4  |
+---+----------------+---+
```
sum() over()应用的SQL：  
```hql
SELECT id,        
       date,       
       pv,
       --默认为从起点到当前行进行累加
       SUM(pv) OVER(PARTITION BY id ORDER BY date) AS pv1,
       --从起点到当前行进行累加，结果同pv1
       SUM(pv) OVER(PARTITION BY id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS pv2,  
       --以id分组内所有行pv累加
       SUM(pv) OVER(PARTITION BY id) AS pv3,
       --当前行+往前3行                               
       SUM(pv) OVER(PARTITION BY id ORDER BY date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS pv4,   
       --当前行+往前3行+往后1行
       SUM(pv) OVER(PARTITION BY id ORDER BY date ROWS BETWEEN 3 PRECEDING AND 1 FOLLOWING) AS pv5,    
       ---当前行+往后所有行S
       UM(pv) OVER(PARTITION BY id ORDER BY date ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS pv6    
FROM data;
```
运行结果：
```hql
+---+----------------+---+----+----+----+----+----+----+
|id |date            |pv |pv1 |pv2 |pv3 |pv4 |pv5 |pv6 |
+---+----------------+---+----+----+----+----+----+----+
|1  |2015-04-10      |1  |1.0 |1.0 |13.0|1.0 |6.0 |13.0|
|1  |2015-04-11      |5  |6.0 |6.0 |13.0|6.0 |13.0|12.0|
|1  |2015-04-12      |7  |13.0|13.0|13.0|13.0|13.0|7.0 |
|2  |2015-04-13      |3  |3.0 |3.0 |9.0 |3.0 |5.0 |9.0 |
|2  |2015-04-14      |2  |5.0 |5.0 |9.0 |5.0 |9.0 |6.0 |
|2  |2015-04-15      |4  |9.0 |9.0 |9.0 |9.0 |9.0 |4.0 |
|3  |2015-04-16      |4  |4.0 |4.0 |4.0 |4.0 |4.0 |4.0 |
+---+----------------+---+----+----+----+----+----+----+
```
注：这些窗口的划分都是在分区内部进行。**如果不指定ROWS BETWEEN，默认统计窗口为从起点到当前行。**

## row_number() over() 
为查询出来的每一行记录生成一个序号。  
序号从1开始，按照顺序，生成分组内记录的序列，row_number()的值不会存在重复，当排序的值相同时，按照表中记录的顺序进行排列。  
示例：利用row_number函数，对表中的数据根据id进行分组，按照pv倒序排序求最大的pv相关信息。  
```hql
select tmp.id,
       tmp.date,
       tmp.pv
from (
         select id,
                date,
                pv,
                row_number() over (partition by id order by pv desc) as rn
         from data
     ) tmp
where tmp.rn = 1;
```
结果：
```hql
+---+----------------+---+
|id |date            |pv |
+---+----------------+---+
|1  |2015-04-12      |7  |
|2  |2015-04-15      |4  |
|3  |2015-04-16      |4  |
+---+----------------+---+
```

## rank() 和 dense_rank() 
1、rank对查询出来的记录进行排名。与row_number函数不同的是，rank函数考虑到了over子句中排序字段值相同的情况，如果使用rank函数来生成序号，over子句中排序字段值相同的序号是一样的，
后面字段值不相同的序号将跳过相同的排名号排下一个，也就是相关行之前的排名数加一，可以理解为根据当前的记录数生成序号，后面的记录依此类推。  
2、dense_rank功能与rank函数类似，但dense_rank函数在生成序号时是连续的。dense_rank函数出现相同排名时，将不跳过相同排名号。  

rank值紧接上一次的rank值。在各个分组内，rank()是跳跃排序，有两个第一名时接下来就是第三名，dense_rank()是连续排序，有两个第一名时仍然跟着第二名。  

示例：  

数据如下：
```hql

+---+----------------+---+
|id |date            |pv |
+---+----------------+---+
|1  |2015-04-10      |1  |
|1  |2015-04-11      |5  |
|1  |2015-04-12      |7  |
|2  |2015-04-13      |3  |
|2  |2015-04-14      |4  |
|2  |2015-04-15      |4  |
|3  |2015-04-16      |4  |
+---+----------------+---+
```
结果：
```hql
+---+----------------+---+----+----------+
|id |date            |pv |rank|dense_rank|
+---+----------------+---+----+----------+
|1  |2015-04-12      |7  |1   |1         |
|1  |2015-04-11      |5  |2   |2         |
|1  |2015-04-10      |1  |3   |3         |
|2  |2015-04-14      |4  |1   |1         |
|2  |2015-04-15      |4  |1   |1         |
|2  |2015-04-13      |3  |3   |2         |
|3  |2015-04-16      |4  |1   |1         |
+---+----------------+---+----+----------+
```

row_number、rank和dense_rank的对比：  

**相同点：**   
都是分组排序     
**不同点：**  
row_number：即便出现相同的排序，排名也不会一致，只会进行累加；集 会根据顺序计算（顺序编号，经常使用，唯一标记一条记录）。1,2,3,4,5...  
rank：当出现相同的排序时，中间会出现一个空缺，即分组内会出现同一个排名，但是排名次序是不连续的。1,1,3,4,4,6...  
dense_rank：当出现相同排序时，中间不会出现空缺，即分组内可能会出现同样的次序，且排序名次是连续的。1,1,1,2,3,4,4,5...  


## first_value() over() 获取数据窗口的第一行某字段值
含义：取分组内排序后，截止到当前行，第一个值。  
示例：按部门分组，统计每个部门员工工资以及该部门最低的员工工资。  
数据（后面几个开窗函数也会用到这些数据）：  
```hql

+-------+-------+---------+------+----------+
|name   |dept_no|employ_id|salary|entry_time|
+-------+-------+---------+------+----------+
|mike   |1      |1        |10000 |2014-01-29|
|tom    |1      |2        |8000  |2013-10-02|
|john   |1      |3        |6000  |2014-10-02|
|jerry  |2      |4        |6600  |2012-11-03|
|jack   |2      |5        |5000  |2010-01-03|
|rose   |2      |6        |4000  |2014-11-29|
|steven |3      |7        |5000  |2014-12-02|
|richard|3      |8        |9000  |2013-11-03|
+-------+-------+---------+------+----------+
```
SQL：
```hql
SELECT name,
       dept_no,
       salary,
       first_value(salary) OVER (PARTITION BY dept_no ORDER BY salary asc) as fv
FROM data;
```
结果：
```hql
+-------+-------+------+----+
|name   |dept_no|salary|fv  |
+-------+-------+------+----+
|john   |1      |6000  |6000|
|tom    |1      |8000  |6000|
|mike   |1      |10000 |6000|
|rose   |2      |4000  |4000|
|jack   |2      |5000  |4000|
|jerry  |2      |6600  |4000|
|steven |3      |5000  |5000|
|richard|3      |9000  |5000|
+-------+-------+------+----+
```

## last_value() over() 获取数据窗口的最后一行某字段值
取分组内排序后，截止到当前行，最后一个值。  
但是使用last_value需要特别注意，这涉及到上述所说的window子句。先看一个例子：    
按部门分组，统计每个部门员工工资以及该部门最高的员工工资。  
执行如下SQL：  
```hql
SELECT name,
       dept_no,
       salary,
       last_value(salary) OVER (PARTITION BY dept_no ORDER BY salary asc) as lv
FROM data;
```
结果：
```hql
+-------+-------+------+-----+
|name   |dept_no|salary|lv   |
+-------+-------+------+-----+
|john   |1      |6000  |6000 |
|tom    |1      |8000  |8000 |
|mike   |1      |10000 |10000|
|rose   |2      |4000  |4000 |
|jack   |2      |5000  |5000 |
|jerry  |2      |6600  |6600 |
|steven |3      |5000  |5000 |
|richard|3      |9000  |9000 |
+-------+-------+------+-----+
```
正确结果dept_no为1、2、3的部门，最高工资应该分别是10000、6600、9000才对，为什么会出现上述结果呢？  
**这是因为last_value默认的窗口是ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW，表示当前行永远是最后一个值，  
需改成ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING。**  
```hql
SELECT name,
       dept_no,
       salary,
       last_value(salary)
                  OVER (PARTITION BY dept_no ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as lv
FROM data;
```
结果：
```hql
+-------+-------+------+-----+
|name   |dept_no|salary|lv   |
+-------+-------+------+-----+
|john   |1      |6000  |10000|
|tom    |1      |8000  |10000|
|mike   |1      |10000 |10000|
|rose   |2      |4000  |6600 |
|jack   |2      |5000  |6600 |
|jerry  |2      |6600  |6600 |
|steven |3      |5000  |9000 |
|richard|3      |9000  |9000 |
+-------+-------+------+-----+
```
此外：   
RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW：为默认值，即当指定了ORDER BY从句，而省略了window从句 ，表示从开始到当前行（当前行永远是最后一个值）。  
RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING：表示从当前行到最后一行。  
RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING：表示所有行。  
n  PRECEDING  m FOLLOWING：表示窗口的范围是[（当前行的行数）- n, （当前行的行数）+ m] row。  

## cume_dist() over()
如果按升序排列，则统计：小于等于当前值的行数/总行数(number of rows ≤ current row)/(total number of rows)。  
如果是降序排列，则统计：大于等于当前值的行数/总行数。  
示例：  
1. 统计小于等于当前工资的人数占总人数的比例。  
```hql
SELECT name,
       dept_no,
       salary,
       cume_dist() OVER (ORDER BY salary) as cume_dist -- 整张表作为一个窗口
FROM data;
```
结果：
```hql
+-------+-------+------+---------+
|name   |dept_no|salary|cume_dist|
+-------+-------+------+---------+
|rose   |2      |4000  |0.125    |
|jack   |2      |5000  |0.375    |
|steven |3      |5000  |0.375    |
|john   |1      |6000  |0.5      |
|jerry  |2      |6600  |0.625    |
|tom    |1      |8000  |0.75     |
|richard|3      |9000  |0.875    |
|mike   |1      |10000 |1.0      |
+-------+-------+------+---------+
```
如果要统计大于等于当前工资的人数占总人数的比例，只需更改salary排序规则为降序desc即可。  

2. 根据部门统计小于等于当前工资的人数占部门总人数的比例  
```hql
SELECT name,
       dept_no,
       salary,
       cume_dist() OVER (PARTITION BY dept_no ORDER BY salary) as cume_dist -- 按部门开窗
FROM data;
```

## lead(列名, offset, [default_value]) over()
用于统计窗口内往下第n行值。  
第一个参数为列名，  
第二个参数offset为往下第n行（可选，默认为1），  
第三个参数为default_value（当往下第n行为NULL时的默认值，如不指定则为NULL。）    
示例：根据部门分组，统计每个部门员工的工资以及大于等于该员工工资的下一个员工的工资。  
```hql
SELECT name,
       dept_no,
       salary,
       lead(salary, 1) OVER (PARTITION BY dept_no ORDER BY salary) as lead
FROM data;
```
结果：
```hql
+-------+-------+------+-----+
|name   |dept_no|salary|lead |
+-------+-------+------+-----+
|john   |1      |6000  |8000 |
|tom    |1      |8000  |10000|
|mike   |1      |10000 |null |
|rose   |2      |4000  |5000 |
|jack   |2      |5000  |6600 |
|jerry  |2      |6600  |null |
|steven |3      |5000  |9000 |
|richard|3      |9000  |null |
+-------+-------+------+-----+
```

## lag(列名, offset, [default_value]) over()
与lead相反，用于统计窗口内往上第n行值。  
第一个参数为列名，  
第二个参数offset为往上第n行（可选，默认为1），  
第三个参数为default_value（当往上第n行为NULL时的默认值，如不指定则为NULL）。  
示例：根据部门分组，统计每个部门员工的工资以及小于等于该员工工资的上一个员工的工资。  
```hql
SELECT name,
       dept_no,
       salary,
       lag(salary, 1) OVER (PARTITION BY dept_no ORDER BY salary) as lag
FROM data;
```
结果：
```hql
+-------+-------+------+----+
|name   |dept_no|salary|lag |
+-------+-------+------+----+
|john   |1      |6000  |null|
|tom    |1      |8000  |6000|
|mike   |1      |10000 |8000|
|rose   |2      |4000  |null|
|jack   |2      |5000  |4000|
|jerry  |2      |6600  |5000|
|steven |3      |5000  |null|
|richard|3      |9000  |5000|
+-------+-------+------+----+
```

## ntile(n) over() 分桶函数，类似于hive中的分桶策略

NTILE(n)，把**有序分区**(必须有order by)中的行分发到指定数据的组中，各个组有编号，编号从1开始，对于每一行，ntile返回此行所属的组的编号。    
将一个有序的数据集划分为多个桶(bucket)，并为每行分配一个适当的桶数。它可用于将数据划分为相等的小切片，为每一行分配该小切片的数字序号。  
NTILE不支持ROWS BETWEEN，比如NTILE(2) OVER(PARTITION BY dept_no ORDER BY salary ROWS BETWEEN 3 PRECEDING - AND CURRENT ROW)。  
**如果切片不均匀，默认增加第一个切片的分布**。  

例如：
```hql
SELECT name,
       dept_no,
       salary,
       ntile(2) over (order by salary)                      n1,-- 全局按照salary升序排列，数据切成3份
       ntile(2) over (partition by dept_no order by salary) n2, -- 按照dept_no分组，在分组内按照salary升序排列,数据切成2份
       ntile(3) over (partition by dept_no order by salary) n3 -- 按照dept_no分组，在分组内按照salary升序排列,数据切成3份
FROM data;
```
这个函数用什么应用场景呢？假如我们想要统计一个cookie，pv数最多的前1/3的天。  
```hql
SELECT id,
       date,
       pv,
       NTILE(3) OVER (PARTITION BY id ORDER BY pv DESC) AS rn
FROM data;
```
rn = 1的记录，就是我们想要的结果。  
```hql
+---+----------------+---+---+
|id |date            |pv |rn |
+---+----------------+---+---+
|1  |2015-04-12      |7  |1  |
|1  |2015-04-11      |5  |2  |
|1  |2015-04-10      |1  |3  |
|2  |2015-04-14      |4  |1  |
|2  |2015-04-15      |4  |2  |
|2  |2015-04-13      |3  |3  |
|3  |2015-04-16      |4  |1  |
+---+----------------+---+---+
```
注意：NTILE、ROW_NUMBER、RANK、DENSE_RANK等序列函数不支持window子句。  

