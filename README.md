# EventDB
> 面向半结构化数据的多维数据库系统

## 安装和运行
1. 配置实验环境 ./config.properties
2. 编译打包: mvn package -Dmaven.test.skip=true; 打包生成target/eventdb-1.0.0.jar
3. 第一次运行: ./eventdb init
4. 新建eventdb格式的数据表：./eventdb createTable tableName initialSplitNumber; createTable是建表命令，tableName是数据表名称，initialSplitNumber指定数据表的初始分区，百亿的数据量一般指定为100，千亿的数据量一般指定为500，万亿的数据量可以指定为2000。
5. 导入数据：执行命令：./eventdb insertHeFits /path/of/fitsfile tableName threadNumber。 insertHeFits是导入高能fits数据的命令，/path/of/fitsfile可以是fits文件，也可以是fits文件夹，tableName是之前新建的eventdb数据表, threaderNumber是并行导入的线程数，一般是cpu总线程数的1/5，而且很耗内存，一般一个线程就会占4~5G内存，所以运行的时候确保正确设置了jvm参数。
6. shell查询：运行./eventdb HeFitsQuery tableName进入shell界面，输入quit退出shell界面。查询命令是：time = timeStart ~ timeEnd & [detID | channel | pulse | eventType] = start ~ end | value1, value2 ... 查询的时候必须指定时间区间，目前在detID, channel, pulse, eventType四个属性上做了索引，所以可以组合查询。

## 配置文件
./config.properties是项目配置文件，配置内容就是hbase，hdfs的配置项
fs.user是hbase.rootdir文件夹的用户名，主要解决权限问题
```shell
#hbase configuration
hbase.zookeeper.property.clientPort = 2181
hbase.zookeeper.quorum = emr17fb191d,emrbf554c6f,emre69bf7ed
hbase.hregion.max.filesize = 21474836480
hbase.hregion.filesize.threshold = 8589934592
hbase.client.keyvalue.maxsize = 524288000
hbase.master = emr17fb191d:16000
zookeeper.znode.parent = /hbase-unsecure

#hdfs
hbase.rootdir = /apps/hbase/data
fs.defaultFS = hdfs://emr17fb191d:8020
fs.user = hbase

#fits
fits.timeBucketInterval = 60.0
fits.meta.table = eventdb_fits_meta
```


## 查询demo
1. time=178789800\~178807800&detID=1\&channel=12\&pulse=0\~255\&eventType=0   查询2017/8/31 15:50:00 到 2017/8/31 20:50:00 1号探测器，12号通道， 0号事件， 在这五个小时采集事例的脉冲跨度的变化情况
2. time=178797000\~178797005\&detID=0\~255\&channel=0\~255&pulse=97\&eventType=0\~255   查询2017/8/31 15:50:00 到 2017/8/31 20:50:00 在这五个小时脉冲宽度等于97的所有事例

## 编程API
eventdb能提供各种编程api, 简单介绍一下java api。拿查询“time = 178890900 ~ 178891500 & detID = 8 & eventType = 1”举例，编程代码如下：
```java
import org.osv.eventdb.fits.HeQueryClient;

HeQueryClient client = new HeQueryClient("HeFits0301");
byte[] events = client.query("time = 178890900 ~ 178891500 & detID = 8 & eventType = 1");
//返回事例的字节码，高能HeFits，16字节一个事例，events.length / 16就是返回的事例数。
```

### 底层api
```java
// 配置文件默认读取 ./config.properties
// 配置文件对象
ConfigProperties conp = new ConfigProperties("your/config/path");
// 生成多维查询数据库对象
MDQuery md = new MDQuery(conp, "tableName");
// 生成多维查询客户端
FitsQueryClient client = new HeQueryClient(md);

// 如果默认用 ./config.properties 配置文件，可以简单申明
FitsQueryClient client = new HeQueryClient("tableName");

// 查询条件对象 FitsQueryFormater 可以设置查询条件
FitsQueryFormater formater = new HeQueryFormater();
// 时间范围设置，参数是开始时间，和结束时间，doule类型
formater.setTimeRange(double startTime, double endTime);
// 设置属性查询条件：范围查询, 参数都是字符串，例子： formater.setPropertyRange("detID", "2", "13")
formater.setPropertyRange(String property, String start, String end);
// 设置属性查询条件：范围列表
// 例子：formater.setPropertyList("detID", new ArrayList(new String[]{"1", "2", "7"}))
formater.setPropertyList(String property, List<String> list);

// 设置好查询条件以后
List<byte[]> result = client.query(formater);
// 就能得到查询结果

// 解析
import org.osv.eventdb.fits.util.HeEventDecoder;
List<HeEventDecoder.He> heList = HeEventDecoder.decode(result);
//HeEventDecoder.He 查看 org.osv.eventdb.fits.util.HeEventDecoder.He

```

### RestFul API
启动http服务
./eventdb server 8081，rest api例子: http://host:8081/he/tableName?time=178797000~178797005&detID=1,2 返回csv格式查询结果
http://host:8081 为网址可视化界面

#### 服务状态
有java和http接口，java接口是org.osv.eventdb.fits.FitsEventDBClient，下面介绍http接口:（返回txt格式)

查看所有的eventdb表：http://host:8081/eventdb:info?op=tableList

eventdb所有事例数：http://host:8081/eventdb:info?op=totalEvents

eventdb所有文件数：http://host:8081/eventdb:info?op=totalFiles

demo表的所有事例数：http://host:8081/eventdb:info?op=eventsOfTable&table=demo

demo表的所有文件数：http://host:8081/eventdb:info?op=filesOfTable&table=demo

demo表的文件列表：http://host:8081/eventdb:info?op=fileListOfTable&table=demo

过去seconds秒的写入速度(单位: 事例/s)，每秒一个采集点，逗号分隔:
http://host:8081/eventdb:info?op=writeSpeedOfLast&lastSeconds=seconds

s ~ e 之间的写入速度， s和e是单位为毫秒的timeStamp， 每秒一个采集点，逗号分隔:
http://host:8081/eventdb:info?op=writeSpeed&startTimeStamp=s&endTimeStamp=e

过去seconds秒的查询延时(单位: ms)，每秒一个采集点，逗号分隔:
http://host:8081/eventdb:info?op=readLatencyOfLast&lastSeconds=seconds

s ~ e 之间的查询延时(单位: ms)， s和e是单位为毫秒的timeStamp， 每秒一个采集点，逗号分隔:
http://host:8081/eventdb:info?op=readLatency&startTimeStamp=s&endTimeStamp=e
