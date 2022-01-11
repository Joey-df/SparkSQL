# Hive常用性能优化方法
https://www.cnblogs.com/frankdeng/p/9463897.html

Apache Hive作为处理大数据量的大数据领域数据建设核心工具，
数据量往往不是影响Hive执行效率的核心因素，
数据倾斜、job数分配的不合理、磁盘或网络I/O过高、MapReduce配置的不合理等等才是影响Hive性能的关键。

Hive在执行任务时，通常会将Hive SQL转化为MapReduce job进行处理。因此对Hive的调优，除了对Hive语句本身的优化，也要考虑Hive配置项以及MapReduce相关的优化。
从更底层思考如何优化性能，而不是仅仅局限于代码/SQL的层面。

## 列裁剪 和 分区裁剪
>什么是列裁剪？
对于没用到的列，则没有必要读取它们的数据去浪费无谓的IO，
列裁剪通过只读取需要的数据，减少IO操作来达到优化的目的。

Hive在读数据的时候，只读取查询中所需要用到的列，而忽略其它列。例如，若有以下查询：
```hql
SELECT age, name FROM people WHERE age > 30;
```
在实施此项查询中，people表有3列（age，name，address），Hive只读取查询逻辑中真正需要的两列age、name，而忽略列address；这样做节省了读取开销，中间表存储开销和数据整合开销。

同理，对于Hive分区表的查询，我们在写SQL时，通过指定实际需要的分区，可以减少不必要的分区数据扫描
【当Hive表中列很多或者数据量很大时，如果直接使用select * 或者不指定分区，效率会很低下（全列扫描和全表扫描）】。
>Hive中与列裁剪和分区裁剪优化相关的配置参数分别为：hive.optimize.cp和hive.optimize.pruner，默认都是true。

## 谓词下推
在关系型数据库如MySQL中，也有谓词下推（Predicate Pushdown，PPD）的概念。它就是将SQL语句中的where谓词逻辑都尽可能提前执行，减少下游处理的数据量。
如下Hive SQL语句：
```hql
select
    a.*,
    b.* 
from 
    a join b on (a.id = b.id)
where a.id > 15 and b.num > 16;
```
如果没有谓词下推，上述SQL需要在完成join处理之后才会执行where条件过滤。在这种情况下，参与join的数据可能会非常多，从而影响执行效率。
使用谓词下推，那么where条件会在join之前被处理，参与join的数据量减少，提升效率。
在Hive中，可以通过将参数hive.optimize.ppd设置为true，启用谓词下推。与它对应的逻辑优化器是PredicatePushDown。该优化器就是将OperatorTree中的FilterOperator向上提。

## Hive优化
### 一、hive.fetch.task.conversion（fetch抓取策略）
(1)、理论分析：
Fetch抓取是指，Hive中对某些情况的查询可以不必使用MapReduce计算（能不启用mapreduce就不启动mapreduce）。
例如：SELECT * FROM emp;在这种情况下，Hive可以简单地读取emp对应的存储目录下的文件，然后输出查询结果到控制台。
在hive-default.xml.template文件中hive.fetch.task.conversion默认是more，老版本hive默认是minimal，该属性修改为more以后，在全局查找、字段查找、limit查找等都不走mapreduce。
```hql
<property>
    <name>hive.fetch.task.conversion</name>
    <value>more</value>
    <description>
      Expects one of [none, minimal, more].
      Some select queries can be converted to single FETCH task minimizing latency.
      Currently the query should be single sourced not having any subquery and should not have
      any aggregations or distincts (which incurs RS), lateral views and joins.
      0. none : disable hive.fetch.task.conversion
      1. minimal : SELECT STAR, FILTER on partition columns, LIMIT only
      2. more  : SELECT, FILTER, LIMIT only (support TABLESAMPLE and virtual columns)
    </description>
  </property>
```
(2)、案例实操
```hql
（1）把hive.fetch.task.conversion设置成none，然后执行查询语句，都会执行mapreduce程序。
hive (default)> set hive.fetch.task.conversion=none;
hive (default)> select * from emp;
hive (default)> select ename from emp;
hive (default)> select ename from emp limit 3;
（2）把hive.fetch.task.conversion设置成more，然后执行查询语句，如下查询方式都不会执行mapreduce程序。
hive (default)> set hive.fetch.task.conversion=more;
hive (default)> select * from emp;
hive (default)> select ename from emp;
hive (default)> select ename from emp limit 3;
```

### 二、本地模式
Hive 在集群上查询时，默认是在集群上 N 台机器上运行， 需要多个机器进行协调运行，这 个方式很好地解决了大数据量的查询问题。
但是当 Hive 查询处理的数据量比较小时，其实没有必要启动分布式模式去执行，因为以分布式方式执行就涉及到跨网络传输、多节点协调 等，并且消耗资源。
这个时间可以只使用本地模式来执行 mapreduce job，只在一台机器上执行，速度会很快。启动本地模式涉及到三个参数：

可以通过设置hive.exec.mode.local.auto的值为true，来让Hive决定在适当的时候自动启动本地模式的开关（默认为false）。
```hql
set hive.exec.mode.local.auto=true;
```
设置本地MR的最大输入数据量，当输入数据量小于这个值时采用本地MR的方式，默认为134217728，即128M
```hql
set hive.exec.mode.local.auto.inputbytes.max=50000000;
```
设置本地MR的最大输入文件个数，当输入文件个数小于这个值时采用本地MR的方式，默认为4
```hql
set hive.exec.mode.local.auto.input.files.max=10;
```

### 三、Hive的存储、压缩
1、合理利用文件存储格式   
创建表时，尽量使用 orc、parquet 这些列式存储格式，因为列式存储的表，每一列的数据在物理上是存储在一起的，Hive查询时会只遍历需要列数据，大大减少处理的数据量。  
2、压缩的原因  
Hive 最终是转为 MapReduce 程序来执行的，而MapReduce 的性能瓶颈在于网络 IO 和 磁盘 IO，要解决性能瓶颈，最主要的是减少数据量，对数据进行压缩是个好的方式。
压缩 虽然是减少了数据量，但是压缩过程要消耗CPU的，但是在Hadoop中， 往往性能瓶颈不在于CPU，CPU压力并不大，所以压缩充分利用了比较空闲的 CPU。  
3、常用压缩方法对比  
| 压缩格式 | 工具  | 算法  | 文件扩展名 | 是否可切分 |
| --- | --- | --- | --- | --- |
| DEFAULT | 无   | DEFAULT | .deflate | 否   |
| Gzip | gzip | DEFAULT | .gz | 否   |
| bzip2 | bzip2 | bzip2 | .bz2 | 是   |
| LZO | lzop | LZO | .lzo | 否   |
| LZ4 | 无   | LZ4 | .lz4 | 否   |
| Snappy | 无   | Snappy | .snappy | 否   |

4、各个压缩方式所对应的 Class 类：  
| 压缩格式 | 对应的编码/解码器 |
| --- | --- |
| DEFLATE | org.apache.hadoop.io.compress.DefaultCodec |
| gzip | org.apache.hadoop.io.compress.GzipCodec |
| bzip2 | org.apache.hadoop.io.compress.BZip2Codec |
| LZO | com.hadoop.compression.lzo.LzopCodec |
| LZ4 | org.apache.hadoop.io.compress.Lz4Codec |
| Snappy | org.apache.hadoop.io.compress.SnappyCodec |

5、压缩方式的选择  
压缩比率
压缩解压缩速度
是否支持 Split

6、压缩使用  
Job 输出文件按照 block 以 GZip 的方式进行压缩：
```hql
set mapreduce.output.fileoutputformat.compress=true // 默认值是 false
set mapreduce.output.fileoutputformat.compress.type=BLOCK // 默认值是 Record
set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec // 默认值是 org.apache.hadoop.io.compress.DefaultCodec
```
Map 输出结果也以 Gzip 进行压缩：  
```hql
set mapred.map.output.compress=true
set mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.GzipCodec // 默认值是 org.apache.hadoop.io.compress.DefaultCodec 
```
对 Hive 输出结果和中间都进行压缩：  
```hql
set hive.exec.compress.output=true // 默认值是 false，不压缩
set hive.exec.compress.intermediate=true // 默认值是 false，为 true 时 MR 设置的压缩才启用
```

### 四、表join的优化
#### 1、Hive 在多个表的join操作时尽可能多的使用相同的连接键，这样在转换MR任务时会转换成尽量少的MR的任务
（当三个或多个以上的表进行join操作时，如果每个on使用相同的字段连接时只会产生一个mapreduce）。
#### 2、小表进行mapjoin
如果在join的表中，有一张表数据量较小，可以存于内存中，这样该表在和其他表join时可以直接在map端进行，省掉reduce过程，效率高。

2.1、手动Map join:在map端完成join操作
```sql
--SQL方式，在SQL语句中添加MapJoin标记（mapjoin hint）
SELECT  /*+ MAPJOIN(smallTable) */  smallTable.key,  bigTable.value 
FROM  smallTable  JOIN  bigTable  ON  smallTable.key  =  bigTable.key;
```

2.2、开启自动的Map Join
```sql
--通过修改以下配置启用自动的mapjoin：
set hive.auto.convert.join = true;
--（该参数为true时，Hive自动对左边的表统计量，如果是小表就加入内存，即对小表使用Map join）
--相关配置参数：
-- 大表小表的阀值设置（默认25M一下认为是小表）：
set hive.mapjoin.smalltable.filesize=25000000;
--（大表小表判断的阈值，如果表的大小小于该值则会被加载到内存中运行）
set hive.ignore.mapjoin.hint=false;
--（默认值：true；是否忽略mapjoin hint 即mapjoin标记）
```

#### 4、大表join大表
​4.1、空key过滤：有时join超时是因为某些key对应的数据太多，而相同key对应的数据都会发送到相同的reducer上，从而导致内存不够。
此时我们应该仔细分析这些异常的key，很多情况下，这些key对应的数据是异常数据，我们需要在SQL语句中进行过滤。例如key对应的字段为空，操作如下：  

案例实操  
（1）配置历史服务器
配置mapred-site.xml
```hql
<property>
    <name>mapreduce.jobhistory.address</name>
    <value>node21:10020</value>
</property>
<property>
    <name>mapreduce.jobhistory.webapp.address</name>
    <value>node21:19888</value>
</property>
```
启动历史服务器
```
sbin/mr-jobhistory-daemon.sh start historyserver
```
查看jobhistory
```
http://node21:19888/jobhistory
```
（2）创建原始数据表、空id表、合并后数据表
```hql
create table ori(id bigint, time bigint, uid string, keyword string, url_rank int, click_num int, click_url string) row format delimited fields terminated by '\t';
create table nullidtable(id bigint, time bigint, uid string, keyword string, url_rank int, click_num int, click_url string) row format delimited fields terminated by '\t';
create table jointable(id bigint, time bigint, uid string, keyword string, url_rank int, click_num int, click_url string) row format delimited fields terminated by '\t';
```
（3）分别加载原始数据和空id数据到对应表中
```hql
hive (default)> load data local inpath '/opt/module/datas/ori' into table ori;
hive (default)> load data local inpath '/opt/module/datas/nullid' into table nullidtable;
```
（4）测试不过滤空id
```hql
hive (default)> insert overwrite table jointable
select n.* from nullidtable n left join ori o on n.id = o.id;
Time taken: 42.038 seconds
```
（5）测试过滤空id
```hql
hive (default)> insert overwrite table jointable
select n.* from (select * from nullidtable where id is not null ) n left join ori o on n.id = o.id;
Time taken: 31.725 seconds
```
​4.2、空key转换：有时虽然某个key为空对应的数据很多，但是相应的数据不是异常数据，必须要包含在join的结果中，此时我们可以表a中key为空的字段赋一个随机的值，使得数据随机均匀地分不到不同的reducer上。
例如：
案例实操：
不随机分布空null值：
（1）设置5个reduce个数
```hql
set mapreduce.job.reduces = 5;
```
（2）JOIN两张表
```hql
insert overwrite table jointable
select n.* from nullidtable n left join ori b on n.id = b.id;
```
结果：可以看出来，出现了数据倾斜，某些reducer的资源消耗远大于其他reducer。

随机分布空null值
（1）设置5个reduce个数
```hql
set mapreduce.job.reduces = 5;
```
（2）JOIN两张表
```hql
insert overwrite table jointable
select n.* from nullidtable n full join ori o on
case when n.id is null then concat('hive', rand()) else n.id end = o.id;
```
结果：可以看出来，消除了数据倾斜，负载均衡reducer的资源消耗。

### 五、SQL优化
#### 1、使用group by + count 代替 Count(Distinct)
作用：去重统计
数据量小的时候无所谓；
但是当一个表的数据量非常大的时候，会发现一个简单的count(distinct order_no)这种语句跑的特别慢，和直接运行count(order_no)的时间差了很多，于是研究了一下。
先说结论：能使用group by代替distinct就不要使用distinct，例子：  
**实际论证：**
order_snap为订单的快照表 总记录条数763191489，即将近8亿条记录,总大小:108.877GB,存储的是公司所有的订单信息，
表的字段大概有20个,其中订单号是没有重复的,所以在统计总共有多少订单号的时候去重、不去重结果都一样，我们来看看:
统计所有的订单有多少条，一个count函数就可以搞定的sql性能如何。  
**DISTINCT：**
```hql
select count(distinct order_no) from order_snap;
Stage-Stage-1: Map: 396 Reduce: 1 Cumulative CPU: 7915.67 sec HDFS Read: 119072894175 HDFS Write: 10 SUCCESS
Total MapReduce CPU Time Spent: 0 days 2 hours 11 minutes 55 seconds 670 msec
OK
_c0
763191489
Time taken: 1818.864 seconds, Fetched: 1 row(s)
```
**GROUP BY+COUNT**
```hql
select count(t.order_no) from (select order_no from order_snap group by order_no) t;
Stage-Stage-1: Map: 396 Reduce: 457 Cumulative CPU: 10056.7 sec HDFS Read: 119074266583 HDFS Write: 53469 SUCCESS
Stage-Stage-2: Map: 177 Reduce: 1 Cumulative CPU: 280.22 sec HDFS Read: 472596 HDFS Write: 10 SUCCESS
Total MapReduce CPU Time Spent: 0 days 2 hours 52 minutes 16 seconds 920 msec
OK
_c0
763191489
Time taken: 244.192 seconds, Fetched: 1 row(s)
```
时间差出7倍多！关键信息对比：
```hql
# distinct
Stage-Stage-1: Map: 396 Reduce: 1 Cumulative CPU: 7915.67 sec HDFS Read: 119072894175 HDFS Write: 10 SUCCESS
# group by + count
Stage-Stage-1: Map: 396 Reduce: 457 Cumulative CPU: 10056.7 sec HDFS Read: 119074266583 HDFS Write: 53469 SUCCESS
```
使用distinct会将所有的order_no都shuffle到一个reducer里面，这就是我们所说的数据倾斜，都倾斜到一个reducer这样性能能不低么？
再看第二个，直接按订单号分组，起了457个reducer（每个reduce对收到的一组数据，进行**去重**），将数据分布到多台机器上执行，时间当然快了。
>结论:
>distinct只会启动一个reducer，会发生数据倾斜，效率低下；
>group by + count会采用分治的思想，启动多个reducer，每个reducer对一组数据进行去重+count，最后全局count得到结果，效率自然会提高；
>能使用group by代替distinct就不要使用distinct。

#### 2、where条件优化
先where过滤后join on，以减少读取的数据量。
优化前（关系数据库不用考虑会自动优化）
```
select m.cid,u.id from order m join customer u on ( m.cid =u.id ) where m.dt='20180808';
```

优化后(where条件在map端执行而不是在reduce端执行）
```
select m.cid,u.id from (select * from order where dt='20180818') m join customer u on ( m.cid =u.id);
```

#### 3、用 in 来代替join
如果需要根据一个表的字段来约束另为一个表，尽量用in来代替join，因为in 要比join 快。
```
select a.id,a.name from tb1  a join tb2 b on (a.id = b.id);
-- 优化后
select id,name from tb1 where id in (select id from tb2);
```

#### 4、优化 in/exists 语句
虽然经过测验，hive1.2.1 也支持 in/exists 操作，但还是推荐使用 hive 的一个高效替代方案：left semi join
因为left semi join在执行时，对于左表中指定的一条记录，一旦在右表中找到立即停止扫描，效率更高.
比如说：
```hql
select a.id, a.name from a where a.id in (select b.id from b);
select a.id, a.name from a where exists (select id from b where a.id = b.id);
```
应该转换成：
```hql
select a.id, a.name from a left semi join b on a.id = b.id;
```

#### 5、union优化
尽量不要使用union （union 去掉重复的记录）而是使用 union all 然后在用group by 去重。

### 六、参数优化
1、调整抓取策略；让可以不走mapreduce任务的，就不走mapreduce任务
```
hive> set hive.fetch.task.conversion=more;
```
2、开启任务并行执行
Hive会将一个查询转化成一个或者多个阶段。这样的阶段可以是MapReduce阶段、抽样阶段、合并阶段、limit阶段。或者Hive执行过程中可能需要的其他阶段。
默认情况下，Hive一次只会执行一个阶段。不过，某个特定的job可能包含众多的阶段，而这些阶段可能并非完全互相依赖的，也就是说有些阶段是可以并行执行的，这样可能使得整个job的执行时间缩短。
不过，如果有更多的阶段可以并行执行，那么job可能就越快完成。
通过设置参数hive.exec.parallel值为true，就可以开启并发执行。不过，在共享集群中，需要注意下，如果job中并行阶段增多，那么集群利用率就会增加。
```
set hive.exec.parallel=true;
```
解释：当一个sql中有多个job时候，且这多个job之间没有依赖，则可以让顺序执行变为并行执行（一般为用到union all的时候）
 
3、同一个sql允许并行任务的最大线程数 
set hive.exec.parallel.thread.number=8;
 
4、设置jvm重用
JVM重用对hive的性能具有非常大的 影响，特别是对于很难避免小文件的场景或者task特别多的场景，这类场景大多数执行时间都很短。
jvm的启动过程可能会造成相当大的开销，尤其是执行的job包含有成千上万个task任务的情况。
```hql
set mapred.job.reuse.jvm.num.tasks=10; 
```
5、合理设置reduce的数目
```hql
-- 方法1：调整每个reduce所接受的数据量大小
set hive.exec.reducers.bytes.per.reducer=500000000; （500M）
-- 方法2：直接设置reduce数量
set mapred.reduce.tasks = 20
```

6、 开启hive内置的数倾优化机制
```hql
set hive.groupby.skewindata=true;
```

7、Group By  
默认情况下，Map阶段同一Key数据分发给一个reduce，当一个key数据过大时就倾斜了。并不是所有的聚合操作都需要在Reduce端完成，很多聚合操作都可以先在Map端进行部分聚合，最后在Reduce端得出最终结果。
```hql
开启Map端聚合参数设置
（1）是否在Map端进行聚合，默认为True
set hive.map.aggr = true;
（2）在Map端进行聚合操作的条目数目
set hive.groupby.mapaggr.checkinterval = 100000;
（3）有数据倾斜的时候进行负载均衡（默认是false）
set hive.groupby.skewindata = true;
当选项设定为 true，生成的查询计划会有两个MR Job。
第一个MR Job中，Map的输出结果会随机分布到Reduce中，每个Reduce做部分聚合操作，并输出结果，这样处理的结果是相同的Group By Key有可能被分发到不同的Reduce中，从而达到负载均衡的目的；
第二个MR Job再根据预处理的数据结果按照Group By Key分布到Reduce中（这个过程可以保证相同的Group By Key被分布到同一个Reduce中），最后完成最终的聚合操作。
```

8、合并小文件
小文件的产生有三个地方，map输入，map输出，reduce输出，小文件过多也会影响hive的分析效率：  
（1）.设置map输入的小文件合并:
```
set mapred.max.split.size=256000000;  
//一个节点上split的至少的大小(这个值决定了多个DataNode上的文件是否需要合并)
set mapred.min.split.size.per.node=100000000;
//一个交换机下split的至少的大小(这个值决定了多个交换机上的文件是否需要合并)  
set mapred.min.split.size.per.rack=100000000;
//执行Map前进行小文件合并
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
```
（2）.设置map输出和reduce输出进行合并的相关参数：
```
//设置map端输出进行合并，默认为true
set hive.merge.mapfiles = true
//设置reduce端输出进行合并，默认为false
set hive.merge.mapredfiles = true
//设置合并文件的大小
set hive.merge.size.per.task = 256*1000*1000
//当输出文件的平均大小小于该值时，启动一个独立的MapReduce任务进行文件merge。
set hive.merge.smallfiles.avgsize=16000000
```

### 七、合理利用分区分桶
1、分区是将表的数据在物理上分成不同的文件夹，以便于在查询时可以精准指定所要读取的分区目录，从来降低读取的数据量。  
2、分桶是将表数据按指定列的hash散列后分在了不同的文件中，将来查询时，hive可以根据分桶结构，快速定位到一行数据所在的分桶文件，从来提高读取效率。  
如下例就是以 userid 这一列为 bucket 的依据，共设置 32 个 buckets:  
```hql
CREATE TABLE page_view(viewTime INT, userid BIGINT,
 page_url STRING, referrer_url STRING,
 ip STRING COMMENT 'IP Address of the User')
 COMMENT 'This is the page view table'
 PARTITIONED BY(dt STRING, country STRING)
 CLUSTERED BY(userid) SORTED BY(viewTime) INTO 32 BUCKETS
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY '1'
 COLLECTION ITEMS TERMINATED BY '2'
 MAP KEYS TERMINATED BY '3'
 STORED AS SEQUENCEFILE;
```
对于经常join的表，针对join字段进行分桶，这样在join时不必全表扫描。

### 八、启用压缩
压缩job的中间结果数据和输出数据，可以用少量CPU时间节省很多空间，压缩方式一般选择Snappy。
要启用中间压缩，需要设定hive.exec.compress.intermediate为true，
同时指定压缩方式hive.intermediate.compression.codec为org.apache.hadoop.io.compress.SnappyCodec。
另外，参数hive.intermediate.compression.type可以选择对块（BLOCK）还是记录（RECORD）压缩，BLOCK的压缩率比较高。
输出压缩的配置基本相同，打开hive.exec.compress.output即可。

### 九、推测执行
在分布式集群环境下，由于负载不均衡或者资源分布不均等原因，会造成同一个作业的多个job之间运行速度不一致，有些job的运行速度可能明显慢于其他任务，则这些job会拖慢整个作业的执行进度。
为了避免这种情况发生，Hadoop采用了推测执行（Speculative Execution）机制。
"推测执行"机制，根据一定的规则推测出"拖后腿"的任务，并为这样的任务启动一个备份任务，让该任务与原始任务同时处理同一份数据，并最终选用最先成功运行完成任务的计算结果作为最终结果。
Hive同样可以开启推测执行。设置开启推测执行参数（在配置文件mapred-site.xml中进行配置）
```hql
<property>
  <name>mapreduce.map.speculative</name>
  <value>true</value>
  <description>If true, then multiple instances of some map tasks
               may be executed in parallel.</description>
</property>

<property>
  <name>mapreduce.reduce.speculative</name>
  <value>true</value>
  <description>If true, then multiple instances of some reduce tasks
               may be executed in parallel.</description>
</property>
```
关于调优这些推测执行变量，目前还很难给出一个具体建议。
如果用户对于运行时的偏差非常敏感的话，那么可以将这些功能关闭掉。
如果用户因为输入数据量很大而需要执行长时间的map或者reduce task的话，那么启动推测执行造成的浪费是非常巨大。