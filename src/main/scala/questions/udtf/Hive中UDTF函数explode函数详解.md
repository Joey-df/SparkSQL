# Hive中UDTF函数explode详解

官网文档：
```html
https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF  
```

Hive中的函数分为3类,UDF函数、UDAF函数、UDTF函数  
1. UDF：一进一出  
2. UDAF：聚集函数，多进一出，类似于：count/max/min  
3. UDTF：一进多出,如explore()、posexplode()，UDTF函数的时候只允许一个字段  

当我们百度搜索explode()时，经常会出现lateral view + explode相关的文章，很少单独写explode()。  
分别了解ecplode() 与lateral view的各自作用很重要，不然过程都不知道实现的，换个UDTF函数就不会使用了。  
而大部分文章都是explode()与lateral view一起讲解,当我们都不会使用UDTF函数时，不理解该类函数时，不知道仅UDTF函数如何使用、我们也不会真正的使用。  

所以先看第一部分吧

## 一、UDTF函数 explode() 讲解
UDTF函数作用都是输入一行数据，将该行数据拆分、并返回多行数据。不同的UDTF函数只是拆分的原理不同、作用的数据格式不同而已。 
这里详细讲解explode()用法，学会这一个其他的UDTF函数也会使用。   

explode()将一行数据转换成多行数据，**（transform a single input row to multiple output rows.）** 可以用于array和map类型的数据。   

#### 1）explode()用于array的语法如下：
Desc: Explodes an array to multiple rows. Returns a row-set with a single column (col), one row for each element from the array.  
```hql
select explode(arraycol) as newcol from tablename;
#arraycol：arrary数据类型的字段。
#tablename：表名

select explode(array('A','B','C'));
select explode(array('A','B','C')) as col;
select tf.* from (select 0) t lateral view explode(array('A','B','C')) tf;
select tf.* from (select 0) t lateral view explode(array('A','B','C')) tf as col;
```

#### 2）explode()用于map的语法如下：
Desc: Explodes a map to multiple rows. Returns a row-set with a two columns (key,value) , one row for each key-value pair from the input map. (As of Hive 0.8.0.).
```hql
select explode(mapcol) as (keyName,valueName) from tablename;
#tablename：表名
#mapcol：map类型的字段
#keyName：表示key转换成的列名称，用于代表key转换之后的列名。
#valueName：表示value转换成的列名称，用于代表value转换之后的列名称。

select explode(map('A',10,'B',20,'C',30));
select explode(map('A',10,'B',20,'C',30)) as (key,value);
select tf.* from (select 0) t lateral view explode(map('A',10,'B',20,'C',30)) tf;
select tf.* from (select 0) t lateral view explode(map('A',10,'B',20,'C',30)) tf as key,value;
```
explode()用于map类型的数据时，由于map是kay-value结构的，所以它在转换的时候会转换成两列，一列是kay转换而成的，一列是value转换而成的。  

#### 3）以上为explode()函数的用法，此函数存在局限性：
其一：不能关联原有的表中的其他字段。  
其二：不能与group by、cluster by、distribute by、sort by联用。  
其三：不能进行UDTF嵌套。  
其四：不允许选择其他表达式。  

## 二、百度搜索explode()，总会出现lateral view，它们各自的作用是什么？
第一部分对explode()函数做了简单的讲解，知道它作用的数据格式为array和map ,也知道了如何单独使用explode，可能脑袋还是有点懵，下面将结合案例一起学习。  

UDTF函数(如：explode)只能只允许对拆分字段进行访问，即select时只能出现explode作用的字段，不能在选择表中其它的字段，否则会报错。  
但是实际中需求中经常要拆某个字段,然后一起与别的字段一起取。这时就要使用lateral view。  

**lateral view为侧视图**,其实就是用来和像类似explode这种UDTF函数联用的，**lateral view会将UDTF生成的结果放到一个虚拟表中**，然后**这个虚拟表会和输入行进行join**来达到连接UDTF外的select字段的目的。  

不加lateral view的UDTF函数只能提取单个字段拆分,并不能塞回原来数据表中。加上lateral view就可以将拆分的单个字段数据与原始表数据关联上。在使用lateral view的时候需要指定视图别名和生成的新列别名。  

#### 1、udtf + lateral view 格式一
```hql
lateral view udtf(map/array) tableAlias as columnAlias
```
>lateral view写在UDTF前，表示连接UDTF所分裂的字段。  
UDTF(expression)：使用的UDTF函数，例如explode()。  
tableAlias：表示UDTF函数转换的虚拟表的名称。  
columnAlias：表示虚拟表的虚拟字段名称，如果分裂之后有一个列，则写一个即可；如果分裂之后有多个列(如map)，按照列的顺序在括号中声明所有虚拟列名，以逗号隔开。  

#### 2、udtf + lateral view 格式二
```hql
from basetable (lateral view)* //表示多个lateral view
```
>在from子句中使用，一般和格式一搭配使用，这个格式只是说明了lateral view的使用位置。  
from子句后面也可以跟多个lateral view语句，使用 **空格** 隔开就可以了，**结果是笛卡尔积**。  
```hql
eg:
SELECT myCol1, myCol2 FROM baseTable
LATERAL VIEW explode(col1) tf1 AS myCol1
LATERAL VIEW explode(col2) tf2 AS myCol2;
#col1为表baseTable字段中的map或者array类型
#col2为表baseTable字段中的map或者array类型

结果是myCol1和myCol2组成的笛卡尔积。
```
#### 3、udtf + lateral view 格式三
```hql
from basetable (lateral view outer)* //在格式二基础上加outer
```
它比格式二只是多了一个outer，这个outer的作用是在UDTF转换列的时候将其中的空也给展示出来，UDTF默认是忽略输出空的，加上outer之后，会将空也输出，显示为NULL。这个功能是在Hive0.12是开始支持的。


## 三、结合案例讲解 lateral view 与 lateral view outer的区别 
两者的区别:  
主要就是当explode函数里传入的字段值是否为null，lateral view explode(null) tf as id 时，结果不显示任何数据 **(注意是指其他字段的数据也不返回**);   
lateral view outer explode(null) tf as id 时，结果显示其他字段是有数据的，但id显示为null。

原始数据：  
```hql
+----+-----+---+--------+
|name|level|age|    favs|
+----+-----+---+--------+
|  张三|高级-砖石| 12|爬山,跳舞,看书|
|  李四|中级-黄金| 15|  看电视,跳舞|
|  王五|初级-青铜| 34|      游泳|
+----+-----+---+--------+
```

#### 测试1：利用split把字符串变成数组类型传入explode函数中，把一行数据炸开形成多行
```hql
select name,
       age,
       level,
       fav
from user_info
         lateral view explode(split(favs, ",")) tf as fav;

结果：
+----+---+-----+---+
|name|age|level|fav|
+----+---+-----+---+
|  张三| 12|高级-砖石| 爬山|
|  张三| 12|高级-砖石| 跳舞|
|  张三| 12|高级-砖石| 看书|
|  李四| 15|中级-黄金|看电视|
|  李四| 15|中级-黄金| 跳舞|
|  王五| 34|初级-青铜| 游泳|
+----+---+-----+---+
```

#### 测试2：多个lateral view explode
```hql
select name,
       age,
       level,
       favs,
       le,
       fa
from user_info
         lateral view explode(split(level, "-")) tf1 as le
         lateral view explode(split(favs, ",")) tf2 as fa;

结果：
+----+---+-----+--------+---+---+
|name|age|level|    favs| le| fa|
+----+---+-----+--------+---+---+
|  张三| 12|高级-砖石|爬山,跳舞,看书| 高级| 爬山|
|  张三| 12|高级-砖石|爬山,跳舞,看书| 高级| 跳舞|
|  张三| 12|高级-砖石|爬山,跳舞,看书| 高级| 看书|
|  张三| 12|高级-砖石|爬山,跳舞,看书| 砖石| 爬山|
|  张三| 12|高级-砖石|爬山,跳舞,看书| 砖石| 跳舞|
|  张三| 12|高级-砖石|爬山,跳舞,看书| 砖石| 看书|
|  李四| 15|中级-黄金|  看电视,跳舞| 中级|看电视|
|  李四| 15|中级-黄金|  看电视,跳舞| 中级| 跳舞|
|  李四| 15|中级-黄金|  看电视,跳舞| 黄金|看电视|
|  李四| 15|中级-黄金|  看电视,跳舞| 黄金| 跳舞|
|  王五| 34|初级-青铜|      游泳| 初级| 游泳|
|  王五| 34|初级-青铜|      游泳| 青铜| 游泳|
+----+---+-----+--------+---+---+
```

#### 测试3：加入空值后，有无outer的区别
```hql
无outer：
select name,
       age,
       level,
       favs,
       col
from user_info
         lateral view explode(array()) tf1 as col;
结果：
+----+---+-----+----+---+
|name|age|level|favs|col|
+----+---+-----+----+---+
+----+---+-----+----+---+
无任何输出。

有outer：
select name,
       age,
       level,
       favs,
       col
from user_info
         lateral view outer explode(array()) tf1 as col;
结果：
+----+---+-----+--------+----+
|name|age|level|    favs| col|
+----+---+-----+--------+----+
|  张三| 12|高级-砖石|爬山,跳舞,看书|null|
|  李四| 15|中级-黄金|  看电视,跳舞|null|
|  王五| 34|初级-青铜|      游泳|null|
+----+---+-----+--------+----+
```

## 四、lateral view posexplode()
与explode的区别是带上了序号（starting with 0）  
示例：  
```hql
select tf.* from (select 0) t
     lateral view posexplode(array('A','B','C')) tf as pos,val
结果：
+---+---+
|pos|val|
+---+---+
|  0|  A|
|  1|  B|
|  2|  C|
+---+---+

select tf.* from (select 0) t
     lateral view posexplode(map('A',10,'B',20,'C',30)) tf as pos,k,v
结果：
+---+---+---+
|pos|  k|  v|
+---+---+---+
|  0|  A| 10|
|  1|  B| 20|
|  2|  C| 30|
+---+---+---+
```

### lateral view posexplode()的用途
#### 1、repeat + posexplode 实现 行转列，并把索引取出
```hql
select tf.* from (select 0) t
     lateral view posexplode(split(repeat("1#",5), "#")) tf as pos, val
     -- where val = "1"
结果：
+---+---+
|pos|val|
+---+---+
|  0|  1|
|  1|  1|
|  2|  1|
|  3|  1|
|  4|  1|
|  5|   |
+---+---+
```

#### 2、获取连续的日期
获取'2020-11-01', '2020-11-30'之间所有的日期  
```hql
select pos,
       x, -- 其实是个空格
       date_add('2020-11-01', pos) as dynamic_date,
       '2020-11-01'                as start_time,
       '2020-11-30'                as end_time
from (select 0) t
         lateral view
             posexplode(split(space(datediff('2020-11-30', '2020-11-01')), ' ')) tf as pos, x
结果：
+---+---+------------+----------+----------+
|pos|  x|dynamic_date|start_time|  end_time|
+---+---+------------+----------+----------+
|  0|   |  2020-11-01|2020-11-01|2020-11-30|
|  1|   |  2020-11-02|2020-11-01|2020-11-30|
|  2|   |  2020-11-03|2020-11-01|2020-11-30|
|  3|   |  2020-11-04|2020-11-01|2020-11-30|
|  4|   |  2020-11-05|2020-11-01|2020-11-30|
|  5|   |  2020-11-06|2020-11-01|2020-11-30|
|  6|   |  2020-11-07|2020-11-01|2020-11-30|
|  7|   |  2020-11-08|2020-11-01|2020-11-30|
|  8|   |  2020-11-09|2020-11-01|2020-11-30|
|  9|   |  2020-11-10|2020-11-01|2020-11-30|
| 10|   |  2020-11-11|2020-11-01|2020-11-30|
| 11|   |  2020-11-12|2020-11-01|2020-11-30|
| 12|   |  2020-11-13|2020-11-01|2020-11-30|
| 13|   |  2020-11-14|2020-11-01|2020-11-30|
| 14|   |  2020-11-15|2020-11-01|2020-11-30|
| 15|   |  2020-11-16|2020-11-01|2020-11-30|
| 16|   |  2020-11-17|2020-11-01|2020-11-30|
| 17|   |  2020-11-18|2020-11-01|2020-11-30|
| 18|   |  2020-11-19|2020-11-01|2020-11-30|
| 19|   |  2020-11-20|2020-11-01|2020-11-30|
| 20|   |  2020-11-21|2020-11-01|2020-11-30|
| 21|   |  2020-11-22|2020-11-01|2020-11-30|
| 22|   |  2020-11-23|2020-11-01|2020-11-30|
| 23|   |  2020-11-24|2020-11-01|2020-11-30|
| 24|   |  2020-11-25|2020-11-01|2020-11-30|
| 25|   |  2020-11-26|2020-11-01|2020-11-30|
| 26|   |  2020-11-27|2020-11-01|2020-11-30|
| 27|   |  2020-11-28|2020-11-01|2020-11-30|
| 28|   |  2020-11-29|2020-11-01|2020-11-30|
| 29|   |  2020-11-30|2020-11-01|2020-11-30|
+---+---+------------+----------+----------+
```

#### 3、获取连续的月份
假如我们需要获取2020-10-01～2021-07-20之间的所有月份
```hql
SELECT pos,
       date_format(add_months(start_date, pos), 'yyyy-MM') AS year_month,
       start_date,
       end_date
FROM (SELECT '2020-10-03' as start_date,
             '2021-07-20' as end_date) t
         lateral VIEW posexplode(split(space(months_between(end_date, start_date)), ' ')) tf AS pos, val
结果：
+---+----------+----------+----------+
|pos|year_month|start_date|  end_date|
+---+----------+----------+----------+
|  0|   2020-10|2020-10-03|2021-07-20|
|  1|   2020-11|2020-10-03|2021-07-20|
|  2|   2020-12|2020-10-03|2021-07-20|
|  3|   2021-01|2020-10-03|2021-07-20|
|  4|   2021-02|2020-10-03|2021-07-20|
|  5|   2021-03|2020-10-03|2021-07-20|
|  6|   2021-04|2020-10-03|2021-07-20|
|  7|   2021-05|2020-10-03|2021-07-20|
|  8|   2021-06|2020-10-03|2021-07-20|
|  9|   2021-07|2020-10-03|2021-07-20|
+---+----------+----------+----------+
```

#### 4、一行变多行（指定列字段要求严格对应关系）
```hql
原始数据：
"A","1,2,3","a,b,c"
"B","10,20,30","aa,bb,cc"
目标结果：
+----+----+----+
|name|val1|val2|
+----+----+----+
|   A|   1|   a|
|   A|   2|   b|
|   A|   3|   c|
|   B|  10|  aa|
|   B|  20|  bb|
|   B|  30|  cc|
+----+----+----+

select name,
       val1,
       val2
from info
         lateral view posexplode(split(col_num, ",")) tf1 as pos1, val1
         lateral view posexplode(split(col_char, ",")) tf2 as pos2, val2
where pos1 = pos2
```