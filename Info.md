# EventDB
> 面向半结构化数据的多维数据库系统

## 安装和运行
1. 实验环境hadoop2.8.0，hbase1.2.6
2. 编译打包: mvn package; 打包生成target/eventdb-1.0.0.jar
3. 新建eventdb格式的数据表：java -jar target/eventdb-1.0.0.jar createTable tableName initialSplitNumber; createTable是建表命令，tableName是数据表名称，initialSplitNumber指定数据表的初始分区，百亿的数据量一般指定为100，千亿的数据量一般指定为500，万亿的数据量可以指定为2000。
4. 加载协处理器：首先需要把jar包拷贝到hdfs目录下，假设路径是/hdfs/path/to/store/java-jar/，hadoop fs -put target/eventdb-1.0.0.jar /hdfs/path/to/store/java-jar/; 然后执行加载命令, java -jar target/eventdb-1.0.0.jar observer tableName org.osv.eventdb.fits.FitsObserver /hdfs/path/to/store/java-jar/eventdb-1.0.0.jar
5. 导入数据：目前完成了高能fits数据0301的导入，执行命令：java -jar target/eventdb-1.0.0.jar insertHeFits /path/of/fitsfile tableName threadNumber。 insertHeFits是导入高能fits数据的命令，/path/of/fitsfile可以是fits文件，也可以是fits文件夹，tableName是之前新建的eventdb数据表, threaderNumber是并行导入的线程数，一般是cpu总线程数的1/10，而且很耗内存，一般一个线程就会占4~5G内存，所以运行的时候确保正确设置了jvm参数。
6. 查询：目前做了一个查询的shell, 运行java -jar target/eventdb-1.0.0.jar HeFitsQuery tableName进入shell界面，输入quit退出shell界面。查询命令是：time = timeStart ~ timeEnd & [detID | channel | pulse | eventType] = start ~ end | value1, value2 ... 查询的时候必须指定时间区间，目前在detID, channel, pulse, eventType四个属性上做了索引，所以可以组合查询。

## 高能所demo环境
在root@sbd01:/root/eventdb/eventdb-java目录下有已经编译打包好的jar包，hbase数据库中有一个2000亿事例的数据表HeFits0301，可以在这个表上演示和实验。

java -jar eventdb-1.0.0.jar HeFitsQuery HeFits0301 进入shell界面，下面列举了一些查询例子，由于是在控制台中输出，给出的查询时间不包含在控制台中输出的时间：

查询timeStamp 178890900 到 178890910 这十秒内detID是1，2，3三台探测器，脉冲宽度20到90之间的事例: time = 178890900 ~ 178890910 & detID = 1, 2, 3 & pulse = 20 ~ 90

查询timeStamp 178890900 到 178891500 这十分钟内detID是8的探测器，事例类型eventType是1的事例：time = 178890900 ~ 178891500 & detID = 8 & eventType = 1

面临的问题：
1. fits数据的timeStamp怎样转化成真实的时间
2. fits文件有多个版本，怎么处理
3. fits不同文件中有timeStamp相同的情况，怎么处理

## 编程API
shell只是演示作用，eventdb能提供各种编程api, 简单介绍一下java api。拿查询“time = 178890900 ~ 178891500 & detID = 8 & eventType = 1”举例，编程代码如下：
```java
import org.osv.eventdb.fits.HeQueryClient;

HeQueryClient client = new HeQueryClient("HeFits0301");
byte[] events = client.query("time = 178890900 ~ 178891500 & detID = 8 & eventType = 1");
//返回事例的字节码，高能HeFits，16字节一个事例，events.length / 16就是返回的事例数。
```



