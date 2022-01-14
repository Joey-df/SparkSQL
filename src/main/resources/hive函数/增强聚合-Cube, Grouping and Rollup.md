# Enhanced Aggregation, Cube, Grouping and Rollup

## 官方文档
```html
https://cwiki.apache.org/confluence/display/Hive/Enhanced+Aggregation%2C+Cube%2C+Grouping+and+Rollup#EnhancedAggregation,Cube,GroupingandRollup-CubesandRollups
```

### 1、GROUPING SETS
GROUP BY中的GROUPING SETS子句，允许我们在同一个查询中指定多个GROUP BY选项。   
所有的GROUPING SET子句，都可以用UNION连接的几个GROUP BY查询等价替换。
```hql
SELECT a, b, SUM(c) FROM tab1 GROUP BY a, b GROUPING SETS ( (a,b) )

等价于

SELECT a, b, SUM(c) FROM tab1 GROUP BY a, b
```
```hql
SELECT a, b, SUM( c ) FROM tab1 GROUP BY a, b GROUPING SETS ( (a,b), a)

等价于

SELECT a, b, SUM( c ) FROM tab1 GROUP BY a, b
UNION
SELECT a, null, SUM( c ) FROM tab1 GROUP BY a
```
```hql
SELECT a,b, SUM( c ) FROM tab1 GROUP BY a, b GROUPING SETS (a,b)

等价于

SELECT a, null, SUM( c ) FROM tab1 GROUP BY a
UNION
SELECT null, b, SUM( c ) FROM tab1 GROUP BY b
```
```hql
SELECT a, b, SUM( c ) FROM tab1 GROUP BY a, b GROUPING SETS ( (a, b), a, b, ( ) )

等价于

SELECT a, b, SUM( c ) FROM tab1 GROUP BY a, b
UNION
SELECT a, null, SUM( c ) FROM tab1 GROUP BY a, null
UNION
SELECT null, b, SUM( c ) FROM tab1 GROUP BY null, b
UNION
SELECT null, null, SUM( c ) FROM tab1
```

### 2、CUBE and ROLLUP
一般的语法是WITH CUBE/ROLLUP。只能和GROUP BY一起使用。   

### CUBE
CUBE是生成group by后的字段集合所有的子序列。   
一旦我们在一组维度上计算一个CUBE，我们就可以得到这些维度所有子序列group by结果的全集。
```hql
SELECT null, b, SUM( c ) FROM tab1 GROUP BY a, b, c WITH CUBE;

等价于

SELECT null, b, SUM( c ) FROM tab1 GROUP BY a, b, c 
GROUPING SETS ( (a, b, c), (a, b), (b, c), (a, c), (a), (b), (c), ( )).
```

### ROLLUP
ROLLUP子句与group by一起用于计算维度层次结构级别上的聚合。
使用ROLLUP的GROUP BY a、b、c，假设层次结构是“a”，向下钻取到“b”，向下钻取到“c”。
```hql
SELECT null, b, SUM( c ) FROM tab1 GROUP BY a, b, c, WITH ROLLUP;

is equivalent to 

SELECT null, b, SUM( c ) FROM tab1 GROUP BY a, b, c 
GROUPING SETS ( (a, b, c), (a, b), (a), ( )).
```
