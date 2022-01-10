# 问题描述
COUNT(DISTINCT xxx)在hive中很容易造成数据倾斜。针对这一情况，网上已有很多优化方法，这里不再赘述。

但有时，"数据倾斜"又几乎是必然的。我们来举个例子：
假设表detail_sdk_session中记录了访问某网站M的客户端会话信息，
即：如果用户A打开app客户端，则会产生一条会话信息记录在该表中，该表的粒度为"一次"会话，其中每次会话都记录了用户的唯一标示uuid，uuid是一个很长的字符串，假定其长度为64位。

现在的需求是：每天统计当月的活用用户数——"月活跃用户数"（当月访问过app就为活跃用户）。我们以2016年1月为例进行说明，now表示当前日期。

## 最简单的方法
这个问题逻辑上很简单，SQL也很容易写出来，例如：
```hql
SELECT
    COUNT(DISTINCT uuid)
FROM detail_sdk_session t
WHERE t.date >= '2016-01-01' AND t.date <= now;
```
上述SQL代码中，now表示当天的日期。很容易想到，越接近月末，上面的统计的数据量就会越大。
更重要的是，在这种情况下，“数据倾斜”是必然的，因为只有一个reducer在进行COUNT(DISTINCT uuid)的计算，所有的数据都流向唯一的一个reducer，不倾斜才怪。

## 优化1
其实，在COUNT(DISTINCT xxx)的时候，我们可以采用"分治"的思想来解决。
对于上面的例子，首先我们按照uuid的前n位进行group by，并做COUNT(DISTINCT)操作，然后再对所有的COUNT(DISTINCT)结果进行求和。

我们先把SQL写出来，然后再做分析。
```hql
-- 外层SELECT求和
SELECT SUM(mau_part) mau
FROM (
         -- 内层SELECT分别进行COUNT(DISTINCT)计算
         SELECT substr(uuid, 1, 3)                 uuid_part,
                COUNT(DISTINCT substr(uuid, 4)) AS mau_part
         FROM detail_sdk_session
         WHERE partition_date >= '2016-01-01'
           AND partition_date <= now
         GROUP BY substr(uuid, 1, 3)
     ) t;
```
上述SQL中，内层select根据uuid的前3位进行group by，并计算相应的活跃用户数COUNT(DISTINCT)，外层select求和，得到最终的月活跃用户数。
这种方法的好处在于，在不同的reducer各自进行COUNT(DISTINCT)计算，充分发挥hadoop的优势，然后进行求和。

注意，上面SQL中，n设为3，不应过大。为什么n不应该太大呢？

我们假定uuid是由字母和数字组成的：大写字母、小写字母和数字，字符总数为26+26+10=62。
理论上，内层select进行group by时，会有 62^n（每个位置62种选择，总共n个位置所以是62^n）个分组，外层select就会进行 62^n 次求和，所以n不宜过大。
当然，如果数据量十分巨大，n必须充分大，才能保证内层select中的COUNT(DISTINCT)能够计算出来，此时可以再嵌套一层select，这里不再赘述。

## 优化2
其实，很多博客中都记录了使用group by操作代替COUNT(DISTINCT) 操作，但有时仅仅使用group by操作还不够，还需要加点小技巧。

还是先来看一下代码：
```hql
--  第三层SELECT
SELECT SUM(s.mau_part) mau
FROM (
         -- 第二层SELECT
         SELECT tag,
                COUNT(*) mau_part
         FROM (
                  -- 第一层SELECT
                  SELECT uuid,
                         CAST(RAND() * 100 AS BIGINT) tag -- 为去重后的uuid打上标记，标记为：0-100之间的整数。
                  FROM detail_sdk_session
                  WHERE partition_date >= '2016-01-01'
                    AND partition_date <= now
                  GROUP BY uuid -- 通过GROUP BY，保证去重
              ) t
         GROUP BY tag
     ) s;
```
第一层select：对uuid进行去重，并为去重后的uuid打上一个整数标记。
第二层select：按照tag标记进行分组，统计每个tag分组下uuid的个数。
第三层select：对所有分组进行求和。

上面这个方法最关键的是为每个uuid进行标记，这样就可以对其进行分组，分别计数，最后去和。
如果数据量确实很大，也可以增加分组的个数。例如：CAST(RAND() * 1000 AS BIGINT) tag。