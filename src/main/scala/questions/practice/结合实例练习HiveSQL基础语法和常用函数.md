# Hive基础语法与常用函数
https://zhuanlan.zhihu.com/p/154400986

**本节使用的表有：** 

**(1) user_info表**  
字段有用户ID、姓名、性别、年龄、城市、首次激活时间、等级、两个额外字段。  
```hql
user_id     string
user_name   string
sex         string
age         int
city        string
firstActiveTime string
level       string
extra1      string
extra2      map<string,string>
```
字段extra1(string)示例：{"systemtype": "android", "education": "doctor", "marriage_status": "1", "phonebrand": "VIVO"};  
字段extra2(map)示例：systemtype:android,education:doctor,marriage_status:1,phonebrand:VIVO  

**(2) user_trade表**  
字段有：用户姓名、购买数量、价格、支付金额、商品分类、支付时间（时间戳格式）、分区日期
tips：看到partition 应该注意到，这是个分区表，在查询的时候需要对dt进行限定。  
```hql
user_name               string  //用户姓名
piece                   int   //购买数量
price                   double //价格 
pay_amount              double    //支付金额
goods_category          string //商品分类
pay_time                bigint //支付时间（时间戳格式）
dt                      string //分区日期
```

##（一）基础语法
### 1、select 字段名 from 表名 where 条件
（1）选出城市在杭州，性别为女的10名用户姓名  
```hql

```
（2）选出在2017年01月12日，购买的商品品类是shoes的用户名、购买数量、支付金额  
```hql

```

### 2、group by分组函数
>group by的作用主要是分类汇总，常见搭配的聚合函数有：  
count()：计数，count(distinct ....) 去重计数；   
sum()：求和  
avg()：求平均值  
max()：求最大值  
min()：求最小值  
对分组后的数据进行筛选，使用having，返回结果仅为符合having条件的结果。  

（1）2019年一月到四月，每个品类有多少人购买，累积金额是多少  
```hql

```
（2）2019年4月，支付金额超过5万元的用户  
```hql

```
### 3、order by排序函数
>order by是对字段进行升序和降序排序，默认为asc（升序），desc(降序），还可以对多个字段进行排序，如order by 字段名1 asc，字段名2 desc。

（1）2019年4月，支付金额最多的top5用户
```hql

-- order by 后面用的是total_amount，而不是sum(pay_amount）
-- 因为执行顺序是from->where->group by-> having->select->order by ->limit
-- 所以先查询出来，这时候别名是total_amount，所以排序的时候只能用total_amount
```

##（二）常见函数
### 1、日期函数
>时间戳转时间的函数：from_unixtime(时间戳字段,'yyyy-mm-dd hh:mm:ss')  
日期转时间戳的函数：unix_timestamp(string date)  
时间转日期的函数：to_date()  
时间转月份的函数：month()  
时间转年份的函数：year()  
时间转当前日期的函数：current_date()  
计算时间间隔函数：datediff() 用结束日期减去开始的日期  
日期增加函数：date_add(开始日期，增加天数)  
日期减少函数：date_sub(开始日期，减少天数)  

（1）将时间戳转为时间格式
```hql
```
（2）用户的首次激活时间，与2019年5月1日的日期间隔  
注意：因为首次激活时间为时间格式，需要转换为日期格式  
```hql

```
### 2、条件函数  
>case when函数：case when 条件 then 结果 end as '别名'
if函数：常用于只有是、否这种条件的情况

（1）统计以下四个年龄段：20岁以下，20-30岁、30-40岁、40岁以上的用户数  
```hql

```
（2）统计每个性别用户等级高低的分布情况（level大于5为高级）
```hql

```

### 3、字符串函数 
>substr(string A,int start,int len) ：substr一般用来处理日期函数，若不写len，则默认到结尾；  
get_json_object(string json_string,string path)：json_string需要解析的json字段，path用.key取出想要获取的value值  

（1）每个月新激活的用户数
```hql

```
（2）不同手机品牌的用户数
字段extra1(string)：{"systemtype": "android", "education": "doctor", "marriage_status": "1", "phonebrand": "VIVO"} ；  
字段extra2（ma）：systemtype:android,education:doctor,marriage_status:1,phonebrand:VIVO  
提取出extra1和extra2中的手机品牌。  
```hql

```

### 4、综合聚合函数和日期函数的 应用
（1）ELLA用户的2018年的平均支付金额，以及2018年最大的支付日期与最小的支付日期的间隔；    
```hql

```
总结：
>1、sum(),avg(),max(),count(),min()这五个聚合函数之间不可以互相嵌套。 如不允许嵌套组合avg(count(*))；  
2、在 Group by 子句中，Select 查询的列，要么需要是 Group by 中的列，要么得是用聚合函数（比如 sum、count 等）加工过的列。不支持直接引用非 Group by 的列。
这一点和 MySQL 有所区别。


## （三）相关练习
（1）2018年购买的商品品类在两个以上的用户数  
解题思路：  
先找出2018年购买商品品类在两个以上的用户；  
再在外层对用户进行计数。  
```hql

-- 可以在外层的where中对用户数进行限定，但为了优化查询，直接在子查询中直接将商品品类少于两个的用户给过滤掉。
```
（2）用户激活时间在2018年，年龄段在20-30岁和30-40岁的婚姻状况分布（用户数）  
>第一步：先选出激活时间在2018年的用户，并把他们所在的年龄段计算好，并提取出婚姻状况   
第二步：取出年龄段在20-30岁和30-40岁的用户，把他们的婚姻状况转移成可理解的说明；聚合计算，针对年龄段、婚姻状况的聚合 
```hql

```
（3）激活天数距今超过300天的男女分布情况  
```hql

```
（4）不同性别、教育程度的分布情况   
```hql

```
（5）2019年1月1日到2019年4月30日，每个时段的不同品类购买金额分布  
>时间限定在2019年1月1日-4月30日  
每个时段，我用小时来表示  
通过时段，不同品类进行分组  
```hql

```


