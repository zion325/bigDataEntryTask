- [1、介绍](#1-介绍)
- [2、环境部署](#2-环境部署)
- [3、实现过程](#3-实现过程)
    - [3.1 模拟数据](#31-模拟数据)
    - [3.2 flink实时处理](#32-flink实时处理)
    - [3.3 spark离线处理](#33-spark离线处理)
      - [3.3 方案一：存到mysql](#331-方案一)
      - [3.3 方案二：存到HDFS](#332-方案二)
- [4、总结](#4-总结)
  - [4.1 收获](#41-收获)
  - [4.2 问题](#41-问题)

# 1 介绍

#### `entryTask`旨在自己采用成熟开源方案在本地模拟并搭建一整套实时数据流以及离线数据流，熟悉数据服务团队数据工程团队的日常工作。

# 2 环境部署

环境：MacBook Pro macOS Ventura-13.6.7(2.6 GHz 六核Intel Core i7)

#### 本文所使用的相关开源组件：

- python 3.7: (非必要：不是环境依赖，个人习惯用py)

- java 8:

- Scala 2.12：

- MySQL 8:

- Spark-3.0.0: 离线计算

- kafka(3.7.1): 实时数据流存储，接收从python模拟生成的数据；

- zookeeper 3.4.6: kafka依赖组件

- flink(1.13.6): 实时计算引擎

- Hadoop(3.4.0):离线数据存储，接收flume从kafka消费的数据

- hive(3.1.3)：离线计算

- flume(1.11.0)：消费kafka数据并存储于hive表中

- superset(1.5.3):可视化工具, 支持Linux/mac


注：！！！如果要用superset这个工具，建议升级py3.8以上或者用docker安装，3.7以下报错太多了，我虽然成功安装了，但是安装过程过于艰难且错太多，各种包不兼容，我都不知道该怎么写笔记。

```
Username [admin]: xian  

User first name [admin]: w

User last name [user]: xian

pwd:123456
```
其他工具：pycharm、idea

参照官网 或 网上教程安装即可；

zookeeper，kafka，mysql环境也可以用docker部署。

注意：
1. 安装后，添加环境变量；
2. MySQL安装后要记住 `用户名` 和 `密码`；
3. 设置`免密登陆`，离线部分可能需要远程登陆的功能；

mac设置免密登陆：
```python
# 生成公钥和私钥
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa

# 将公钥追加到文件
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys 

# 修改相关权限
chmod 0600 ~/.ssh/authorized_keys 

# 测试
ssh localhost

# 成功标志，类似：
Last login: Thu Jul 18 11:32:23 2024
```

# 3 实现过程

![过程图](images/81721288186_.pic.jpg)

- **实时**：

部署kafka、zookeeper、es、kibana(也可以用docker部署，后两个主要用来可视化，可根据需求部署)

使用python生成模拟数据并将数据灌入kafka

安装flink并连接kafka作为数据源、计算相关指标、连接mysql保存计算结果


- **离线**：

方案一：flink并连接kafka作为数据源，保存到本地文件/HDFS，使用spark计算，结果保存到mysql。(由于模拟数据量较少，本文将源数据也保存到mysql)

方案二：搭建hadoop集群和hive环境，并创建数据源表，搭建flume并消费数据到hive源表，hive计算结果保存到mysql

或者使用spark计算结果，并保存至mysql表

注：

常用的模式：

搭建hadoop集群和hive环境，并建立数据源表

安装flume，编写配置文件，启动flume传输

hiveSQL完成指标计算

## 3.1 模拟数据

#### 前提：启动kafka，创建主题

若使用`brew`安装的kafka，

启动：(先开启zookeeper，再开启kafka)
```

brew services start zookeeper
brew services start kafka
```

创建主题
```
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic test_topic
```

#### **测试：**

生产者
```
kafka-console-producer --bootstrap-server localhost:9092 --topic test_topic
```
启动成功后即可在交互环境中直接输入数据来模拟数据的流入

`Ctrl+C` 退出交互界面


消费者
```
kafka-console-consumer --bootstrap-server localhost:9092 --topic test_topic --from-beginning
```


#### 下面采用python实时产生模拟数据，并将数据存入kafka：

代码：包含两个`.py`文件

```python
# datagenkafka.py

from kafka import KafkaProducer
from kafka.errors import KafkaError

KAFKA_HOST = "localhost"  # 服务器端口地址
KAFKA_PORT = 9092  # 端口号
KAFKA_TOPIC = "entrytask-mockdata-order"  # topic


class Kafka_producer():
    def __init__(self, kafka_host, kafka_port, kafka_topic):
        self.kafkaHost = kafka_host
        self.kafkaPort = kafka_port
        self.kafkaTopic = kafka_topic
        self.producer = KafkaProducer(bootstrap_servers='{kafka_host}:{kafka_port}'.format(
            kafka_host=self.kafkaHost,
            kafka_port=self.kafkaPort
        ))

    def send_data_json(self, data):
        try:
            message = data
            producer = self.producer
            producer.send(self.kafkaTopic, value=message.encode('utf-8'))
            producer.flush()
        except KafkaError as e:
            print(e)


def log_kafka(params):
    print("================producer:=================\n")
    print(params, "\n")
    producer = Kafka_producer(kafka_host=KAFKA_HOST, kafka_port=KAFKA_PORT, kafka_topic=KAFKA_TOPIC)
    producer.send_data_json(params)

```

```python
# main.py

import json
import random
from datagenkafka import log_kafka
import time

# order_id 订单id，15位随机数字，不重复出现
# user_id 用户id,11位随机数字，可重复出现
# order_tz 时区，用户时区，从列表中选择，为简单化仅选择北京时间
# amount 付费金额，0-10000之间随机数字
# currency 货币种类，默认人人民币
# channel_id 渠道ID，1到200中随机数字，可重复
# order_time 订单时间，默认数据产生时间
while (True):
    data = {}
    data['order_id'] = random.randint(100000000000000, 999999999999999)
    data['user_id'] = random.randint(1, 5000)
    data['order_tz'] = 'beijing'
    data['amount'] = random.randint(1, 10000)
    data['currency'] = 'rmb'
    data['channel_id'] = random.randint(0, 200)
    data['order_time'] = int(time.time())
    data = json.dumps(data)
    log_kafka(data)
    sleep_time = random.randint(3, 10)
    time.sleep(sleep_time)

```

运行`main.py`, 就可以自动模拟数据并插入kafka。

可以在pycharm编辑器上看到数据内容

进入终端，启动消费者，查看是否成功插入数据。

## 3.2 flink实时处理

本文采用flink版本为1.13.6

官网下载压缩包后，解压+配置环境变量

由于要使用`sql-client`连接kafka和mysql, 因此要引入相应的依赖包(如下三个)，官网 或者 https://repo1.maven.org/maven2/org/apache/flink/ 下载对于版本依赖包，粘贴到flink安装目录的lib文件夹下

`flink-sql-connector-kafka_2.12-1.13.6.jar`

`flink-connector-jdbc_2.12-1.13.6.jar`

`mysql-connector-j-9.0.0.jar`（这个JDBC可以到mysql官方下载）

启动flink集群：
```
cd flink安装目录        
./bin/start-cluster.sh      # 启动集群
./bin/sql-client.sh embedded    # 打开客户端
```

启动完成后可以访问 http://localhost:8081 查看flink web ui。

`注意`：flink默认的并行任务数量好像是`1`, 如果有多个任务同时跑，需要改配置：

```
vim flink安装目录/conf/flink-conf.yaml 

# 找到taskmanager.numberOfTaskSlots并修改为：
taskmanager.numberOfTaskSlots: 5

:wq         # 保存退出
```
需要重启flink集群后配置才生效

```
./bin/stop-cluster.sh
./bin/start-cluster.sh
```

### 创建表

下面创建的表是临时表，不会持久化，若要持久化，可参考官网实现`catalog`模式；

为了方便，我是把建表语句都放到一个`.sql`文件中，每次打开客户端，用它初始化一遍，注意：表仍然是临时表

```
vim createtable.sql
# 将建表sql语句粘贴
:wq     # 保存退出
./bin/sql-client.sh -i createtable.sql    # 打开客户端命令
```

#### 建表语句

```sql
-- 连接kafka建立源表
CREATE TABLE user_behavior (
    order_id BIGINT,
    user_id BIGINT,
    order_tz STRING,
    amount BIGINT,
    currency STRING,
    channel_id BIGINT,
    order_time BIGINT,
    ts as TO_TIMESTAMP(FROM_UNIXTIME(order_time,'yyyy-MM-dd HH:mm:ss')),
    proctime as PROCTIME(),   -- 通过计算列产生一个处理时间列
    WATERMARK FOR ts as ts - INTERVAL '5' SECOND  -- 在ts上定义watermark，ts成为事件时间列
) WITH (
    'connector' = 'kafka',  -- 使用 kafka connector
    'topic' = 'entrytask-mockdata-order',  -- kafka topic
    'scan.startup.mode' = 'earliest-offset',  -- 从起始 offset 开始读取
    'properties.zookeeper.connect' = 'localhost:2181',  -- zookeeper 地址
    'properties.bootstrap.servers' = 'localhost:9092',  -- kafka broker 地址
    'format' = 'json',  -- 数据源格式为 json
    'property-version' = '1'
);
```

```sql
-- 成交额
CREATE TABLE cumulative_payment_uv (
    date_str STRING,  --日期
    time_str STRING,  --时间
    uv BIGINT,  --累计uv
    payment double,  --累计成交额
    PRIMARY KEY (date_str, time_str) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://localhost:3306/entryTask',
  'table-name' = 'cumulative_payment_uv',
  'username' = 'root',
  'password' = 'wangxian'
);
```

```sql
--每分钟成交量
CREATE TABLE buy_cnt_per_min (
    min_of_day TIMESTAMP(3) PRIMARY KEY  NOT ENFORCED,
    buy_cnt BIGINT
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://localhost:3306/entryTask',
  'table-name' = 'buy_cnt_per_min',
  'username' = 'root',
  'password' = 'wangxian'
);
```

```sql
--用户成交额表
CREATE TABLE payment_user_list (
    user_id BIGINT PRIMARY KEY  NOT ENFORCED,
    amount double
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://localhost:3306/entryTask',
  'table-name' = 'payment_user_list',
  'username' = 'root',
  'password' = 'wangxian'
);
```

```sql
--渠道成交额表
CREATE TABLE payment_channels_list (
    channel_id BIGINT PRIMARY KEY  NOT ENFORCED,
    amount double
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://localhost:3306/entryTask',
  'table-name' = 'payment_channels_list',
  'username' = 'root',
  'password' = 'wangxian'
);
```

### 统计并插入数据
```sql
--当天累计成交量累计uv计算
INSERT INTO cumulative_payment_uv
SELECT date_str, MAX(time_str), COUNT(DISTINCT user_id) as uv, sum(amount) as payment
FROM (
  SELECT
    DATE_FORMAT(ts, 'yyyy-MM-dd') as date_str,
    DATE_FORMAT(ts, 'HH:mm')as time_str,
    user_id,
    amount
  FROM user_behavior)
GROUP BY date_str;
```

```sql
--每分钟成交量计算
INSERT INTO buy_cnt_per_min
SELECT TUMBLE_START(ts, INTERVAL '1' MINUTE), COUNT(*)
FROM user_behavior
GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE);
```

```sql
--用户成交额计算
INSERT INTO payment_user_list
SELECT user_id,sum(amount) as amount
from user_behavior
group by user_id;
```

```sql
--渠道成交额计算
INSERT INTO payment_channels_list
SELECT channel_id,sum(amount) as amount
from user_behavior
group by channel_id;
```

## 3.3 spark离线处理

前提：下载jdbc, 例如`mysql-connector-j-9.0.0.jar`，粘贴到spark安装目录的`jars`文件夹下

### 3.3.1 方案一

在mysql建sink表 
```sql
CREATE EXTERNAL TABLE cumulative (
    dt varchar(255),  --日期
    uv BIGINT,  --uv
    buy_cnt BIGINT, --成交量 
    amount double  --成交额
)
```

python、java、scala都能写spark任务

`Python版`
```python
# 提前pip安装下面这两个包

from pyspark.sql import SparkSession
import findspark

findspark.init()
# 创建 SparkSession
spark = SparkSession.builder \
  .appName("entryTask") \
  .master("local[2]")
  .getOrCreate()

# 加载输入表
inputTable = spark.read \
  .format("jdbc") \
  .option("url", "jdbc:mysql://localhost:3306/entryTask") \
  .option("driver", "com.mysql.cj.jdbc.Driver") \
  .option("dbtable", "user_behavior") \
  .option("user", "root") \
  .option("password", "wangxian") \
  .load()

# 创建临时视图
inputTable.createOrReplaceTempView("input_view")

# 执行 WordCount 查询
import datetime

last_hour = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime("%Y-%m-%d %H")

result = spark.sql("""
  select
    cast(dt as varchar(255)),count(1) as uv,sum(buy_cnt) as buy_cnt,sum(amount) as amount
from 
(
    select substr(from_utc_timestamp(order_time*1000,'PRC'),1,13) as dt,user_id,count(1) as buy_cnt,sum(amount) as amount
    from input_view
    where substr(from_utc_timestamp(order_time*1000,'PRC'),1,13) = '${last_hour}'
    group by substr(from_utc_timestamp(order_time*1000,'PRC'),1,13),user_id
)t1
group by cast(dt as varchar(255));
""")

# 将结果保存到输出表
result.write \
  .format("jdbc") \
  .option("url", "jdbc:mysql://localhost:3306/test") \
  .option("driver", "com.mysql.cj.jdbc.Driver") \
  .option("dbtable", "cumulative") \
  .option("user", "root") \
  .option("password", "wangxian") \
  .mode("overwrite") \
  .save()

# 停止 SparkSession
spark.stop()
```

****
`java版/scala版`

构建maven项目，配置pom.xml, 添加下面内容，然后load即可
```xml
<dependencies>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_2.12</artifactId>
            <version>3.0.0</version>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_2.12</artifactId>
            <version>3.0.0</version>
        </dependency>
        <dependency>
            <groupId>mysql</groupId>
            <artifactId>mysql-connector-java</artifactId>
            <version>8.0.26</version>
        </dependency>
</dependencies>
```

逻辑代码：略。  参考python代码，或者GPT

### 3.3.2 方案二

**1、搭建hadoop和hive，并创建源表**

macbook搭建：(注意设置ssh免密登陆)

```
brew install Hadoop 
brew install hive
```

修改配置文件：在`/usr/local/Cellar/hadoop/3.4.0/libexec/etc/hadoop`目录下，那四个`xx-site.xml`文件

注：配置所有用户**写操作**，不然后面可能报错。

hadoop安装及配置文件参考： https://www.cnblogs.com/micrari/p/5716851.html

先跑样例试试，例如 求pi、wordcount

如果出现类似报错：找不到或无法加载主类 org.apache.hadoop.mapreduce.v2.app.MRAppMaster

参考：https://developer.aliyun.com/article/1146913

**创建hive元数据库**
```sql
-- 在mysql执行
create database metastore;

create user 'hive'@'localhost' identified by '123456';

grant select,insert,update,delete,alter,create,index,references on metastore.* to 'hive'@'localhost';

flush privileges;
```

修改配置文件`hive-site.xml`

```xml
<property>
  <name>javax.jdo.option.ConnectionURL</name>
  <value>jdbc:mysql://localhost/metastore</value>
</property>
 
<property>
  <name>javax.jdo.option.ConnectionDriverName</name>
  <value>com.mysql.jdbc.Driver</value>
</property>
 
<property>
  <name>javax.jdo.option.ConnectionUserName</name>
  <value>hive(填上述mysql中创建的用户名)</value>
</property>
 
<property>
  <name>javax.jdo.option.ConnectionPassword</name>
  <value>123456(填上述mysql中创建的用户密码)</value>
</property>
 
<property>
  <name>hive.exec.local.scratchdir</name>
  <value>/tmp/hive</value>
</property>
 
<property>
  <name>hive.querylog.location</name>
  <value>/tmp/hive</value>
</property>
 
<property>
  <name>hive.downloaded.resources.dir</name>
  <value>/tmp/hive</value>
</property>
 
<property>
  <name>hive.server2.logging.operation.log.location</name>
  <value>/tmp/hive</value>
</property>

<property>
  <name>hive.support.concurrency</name>
  <value>true</value>
  <description>Enable ACID transactions for Hive tables</description>
</property>

<property>
  <name>hive.txn.manager</name>
  <value>org.apache.hadoop.hive.ql.lockmgr.DbTxnManager</value>
  <description>Transaction manager implementation class</description>
</property>

<property>
  <name>hive.compactor.initiator.on</name>
  <value>true</value>
  <description>Enable automatic compaction initiation</description>
</property>

<property>
  <name>hive.compactor.worker.threads</name>
  <value>1</value>
  <description>Number of compaction worker threads</description>
</property>

```

添加JDBC依赖包至hive的lib文件夹下,我这里的包是 `mysql-connector-j-9.0.0.jar`

初始化metastore库

- 命令：`schematool -initSchema -dbType mysql`

启动hadoop集群和yarn：
- 在hadoop的libexec目录下执行`sbin/start-all.sh`和`sbin/start-yarn.sh`。

启动hive metastore：
- `bin/hive --service metastore&`。
- 后台启动：`nohup bin/hive --service metastore&`

由于采用的是flume来将数据传入hive，源表需要分桶并采用orc格式储存。

```sql
-- hive建表语句
-- 分两个桶
create table user_behavior.user_behavior
(    order_id BIGINT,
    user_id BIGINT,
    order_tz STRING,
    amount BIGINT,
    currency STRING,
    channel_id BIGINT,
    order_time BIGINT
)
clustered by(order_id) into 2 buckets stored as orc tblproperties('transactional'='true');
```

注：`tblproperties('transactional'='true')` 要求满足ACID

安装flume:(1.11.0)建议不要安装最新版，可以考虑换1.9
- 命令：`brew install flume`

安装完成后将flume连接hive所需的相关依赖包复制到flume的`lib`文件夹下（注：去hive的lib目录找，找不到就去官网下载，hive 3.0版本将一些类更换了所在的jar包，需要特别注意）

- `calcite-core-1.16.0.3.1.2.jar`

- `libfb303-0.9.3.jar`

- `hive-hcatalog-core-3.1.2.jar`

- `hadoop-mapreduce-client-core-3.1.2.jar`

- `hive-exec-3.1.2.jar`

- `hive-standalone-metastore-3.1.2.jar`

- `hive-hcatalog-streaming-3.1.2.jar`

flume配置文件`user_behavior.conf`

(这里我是在conf目录同级创建了一个myconf目录，`user_behavior.conf`放myconf)

```sh
# agent名：a
a.sources=source_from_kafka
a.channels=mem_channel
a.sinks=hive_sink
 
#kafka为souce的配置
a.sources.source_from_kafka.type=org.apache.flume.source.kafka.KafkaSource
a.sources.source_from_kafka.kafka.bootstrap.servers=localhost:9092
a.sources.source_from_kafka.kafka.topics=user_behavior
a.sources.source_from_kafka.channels=mem_channel
 
#hive为sink的配置
a.sinks.hive_sink.type=hive
a.sinks.hive_sink.hive.metastore=thrift://localhost:9083
a.sinks.hive_sink.hive.database=user_behavior
a.sinks.hive_sink.hive.table=user_behavior
a.sinks.hive_sink.serializer=JSON
a.sinks.hive_sink.serializer.fieldnames=order_id,user_id,order_tz,amount,currency,channel_id,order_time
a.sinks.hive_sink.batchSize=100   # 每个事务中的事件数量
 
#channel的配置
a.channels.mem_channel.type=memory
a.channels.mem_channel.capacity=200
a.channels.mem_channel.transactionCapacity=100
 
#三者之间的关系
a.sources.source_from_kafka.channels=mem_channel
a.sinks.hive_sink.channel=mem_channel
```

启动`flime-ng`并执行传输，需确保kafka和hadoop集群已经启动并且hive已经创建好源表:

(如果没有配置环境变量，要到`flume安装目录/bin`下执行)

```sh
# 启动flume
flume-ng agent --conf conf/ --conf-file conf/….  --name a -Dflume.root.logger=INFO,console -Xmx256m;
# 启动数据传输conf
flume-ng agent --conf /usr/local/Cellar/flume/1.11.0/libexec/myconf/ --conf-file /usr/local/Cellar/flume/1.11.0/libexec/myconf/user_behavior.conf  --name a -Dflume.root.logger=INFO,console -Xmx256m;
```

注：flume进行批量传输，hive客户端查看到的数据可能不是实时更新，可以尝试刷新`msck repair table tableName`或者重启客户端(但是不能停止metastore服务)

（问题：有时候达到了设置的批次阈值也没有刷新，可能是jar包版本不兼容，因为启动的时候出现某个jar包的警告）。

`计算指标`

接下来就是计算指标，然后存入可快速查询的数据库mysql、oracle、es，这里就用mysql。

计算框架：MR、spark，本文用spark

`MR`：可直接中hive客户端创建指标表，和mysql对应，运行hieSQL语句计算结果然后插入指标表，默认hive的计算框架技术MR，导出到mysql可以用sqoop，或者写到本地文件再用mysql load语句加载。(本文不采用这个方案)

`spark`：

这里用本地模式

构建maven项目，在`pom.xml`添加依赖，根据自己的环境调整版本

```xml
<dependencies>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_2.12</artifactId>
            <version>3.0.0</version>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_2.12</artifactId>
            <version>3.0.0</version>
        </dependency>
        <dependency>
            <groupId>mysql</groupId>
            <artifactId>mysql-connector-java</artifactId>
            <version>8.0.26</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hive</groupId>
            <artifactId>hive-jdbc</artifactId>
            <version>3.1.3</version>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-hive_2.12</artifactId>
            <version>3.0.0</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hive</groupId>
            <artifactId>hive-exec</artifactId>
            <version>3.1.3</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hive</groupId>
            <artifactId>hive-standalone-metastore</artifactId>
            <version>3.1.3</version>
        </dependency>
    </dependencies>
```

**上游** - 连接hiveserver2：可以是`spark on hive` 或者 `spark jdbc`，前者需要修改配置文件以及jar包等，后者需要相关的jar包(例如上面的依赖)，本文采用 后者 ，比较简单、灵活。

- 启动`nohup hive --service hiveserver2 &`

**计算** - sparkSQL

**下游** - 连接mysql：需要mysql的jdbc即可

注：不同环境可能出现错误，例如 权限问题、环境变量问题、jar包版本兼容问题，都需要在调试中修改，有两个主要坑点，在下面代码里写了。

权限：hadoop dfs -chmod 777 /path

逻辑代码(`java`):

```java
package org.example;

import java.sql.*;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.*;
import org.apache.spark.sql.jdbc.JdbcDialects;


import java.time.Instant;
import java.util.Arrays;


public class Cumulative {
    public static void main(String[] args) throws AnalysisException {
        // 设置 Hive 连接参数
        String hiveUrl = "jdbc:hive2://localhost:10000/user_behavior";
        String hiveUser = "hive";
        String hivePassword = "123456";

        // 设置 MySQL 连接参数
        String mysqlUrl = "jdbc:mysql://localhost:3306/entryTask";
        String mysqlUser = "hive";
        String mysqlPassword = "123456";

        // 创建 SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("Hive to MySQL")
                .master("local[2]")
                .enableHiveSupport()
                .getOrCreate();

        // 坑点：spark读hive表的时候，可能会自动加单/双引号 或者 反引号，这两句是去掉他们
        HiveSqlDialect hiveSqlDialect = new HiveSqlDialect();
        JdbcDialects.registerDialect(hiveSqlDialect);

        // 从 Hive 中读取数据
        Dataset<Row> hiveData = spark.read()
                .format("jdbc")
                .option("driver", "org.apache.hive.jdbc.HiveDriver")
                .option("url", hiveUrl)
                .option("dbtable", "user_behavior")
                .option("user", hiveUser)
                .option("password", hivePassword)
                .load();


        // 坑点：读出来读数据列名会重命名，加表名前缀，例如：userID --> tableName.userID
        // 手动给他去掉
        for (String oldName : hiveData.columns()) {
            String newName = oldName.replace("user_behavior" + ".", "").replace("'", "");
            hiveData = hiveData.withColumnRenamed(oldName, newName);
        }
        
        hiveData.createTempView("a");


        // 执行指标计算，这里举例计算单位小时内的 用户量uv，购买量buy_count，总金额amount，
        // 可以做成定时任务，以每天执行为例
        long currentTimeSeconds = Instant.now().getEpochSecond();
        // 计算一天前的时间戳（秒）
        long startTime = currentTimeSeconds - 3600*24;
        System.out.println("start: " + startTime);
        Dataset<Row> result = spark.sql("SELECT \n" +
                "CAST(t1.dt AS string) AS dt, \n" +
                "COUNT(t1.user_id) AS uv, \n" +
                "SUM(t1.buy_cnt) AS buy_count, \n" +
                "SUM(t1.amount) AS amount \n" +
                "FROM ( \n" +
                "SELECT \n" +
                "SUBSTR(FROM_UTC_TIMESTAMP(cast(order_time as timestamp),'PRC'), 1, 13) AS dt, \n" +
                "cast(user_id as string) as user_id, \n" +
                "COUNT(1) AS buy_cnt, \n" +
                "SUM(amount) AS amount \n" +
                "FROM  a \n" +
                "WHERE cast(order_time AS bigint) >= "+ startTime +" \n" +
                "GROUP BY FROM_UTC_TIMESTAMP(cast(order_time as timestamp),'PRC'), user_id \n" +
                ") t1 \n" +
                "GROUP BY CAST(t1.dt AS string)" +
                "order by cast(dt AS timestamp) desc" +
                ";");

        result.show();
        // 将结果保存到 MySQL
        result.write().format("jdbc")
                .option("url", mysqlUrl)
                .option("user", mysqlUser)
                .option("password", mysqlPassword)
                .option("dbtable", "cumulative")
                .mode(SaveMode.Append)
                .save();
        // 关闭 SparkSession
        spark.stop();

    }


    public static class HiveSqlDialect extends JdbcDialect {

        @Override
        public boolean canHandle(String url){
            return url.startsWith("jdbc:hive2");
        }

        @Override
        public String quoteIdentifier(String colName) {
            return colName.replace("\"","");
        }

    }
}

```

运行`main`函数，查看mysql的对应表，有数据而且和控制台打印结果一致，成功！！！



# 4 总结

## 4.1 收获
1. 在本地搭建一个小型实时和离线数据流计算的框架，熟悉了各个组件的基本逻辑，也对数据平台的搭建有了更深的认识。

2. 学会如何阅读官方文档以及如何解决过程中的各种细小问题。

3. 学会参考官方文档，调整解决组件之间版本兼容问题。

4. 学习了新组件的使用，kafka，flink，flume。

5. 锻炼代码能力，实现过程中，用不同编程语言实现，提升了代码能力。

## 4.1 问题

1. 最终搭建的平台还缺少了很多重要组件，es、可视化组件等

2. 对组件的理解不够深刻，对组件的应用也不熟练，整体框架还有很大优化空间。

3. 只部署本地单一节点模式，没有副本和容错，许多复杂问题没能实践。

4. 整体框架流程较为简单，例如没有`清洗`、`维度退化和构建`、`数据监控`、`验证`等过程。



