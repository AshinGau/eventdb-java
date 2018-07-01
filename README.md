# eventdb
> EventDB:基于HBase的半结构化数据的多维索引系统

半结构化数据没有固定的模式结构，需要存储和检索的数据量往往也达到了万亿条记录，所以传统的关系型数据库无法满足存储和检索需求。HBase是分布式的、可伸缩的非关系型数据库，但只支持Key-Value的检索查询，EventDB基于HBase实现半结构化数据的多维检索。本论文的研究工作是基于国家重点研发计划“科学大数据管理系统”，运行和测试的数据集是高能物理大数据。高能物理以事例为单位存储一次高能物理实验记录，包含脉冲宽度、轨迹、能量等属性，检索时需要多个维度组合查询。目前，北京正负电子对撞机和慧眼卫星累计的事例数就多达万亿级。
本论文的主要工作如下：

1. EventDB架构设计  
	为了实现万亿级事例的检索，和其它大数据系统一样，EventDB采用分片的方法管理和检索数据。数据的分片策略可以根据数据的特点制定，北京正负电子对撞机的ROOT数据可以按Run分片，慧眼卫星是时序数据，可以按时间窗口分片。一个分片内的数据，使用位图BitMap对数据进行索引。

2. 基于HBase的实现  
	HBase按Key-Value的形式存储数据，以Region为单位存储和管理分区数据。多维索引的实现需要解决数据分片如何映射到HBase的数据分区上，保证索引和原始数据的本地性(Locality)，并解决大数据面临的数据倾斜的共性问题。

3. 实验分析和测试对比  

## 多维索引的设计
HBase只支持Key-Value的检索查询，为了实现万亿级事例的多维度组合查询，EventDB采用了分片的方法管理和检索数据。数据的分片策略可以根据数据的特点制定，北京正负电子对撞机的ROOT数据可以按Run分片，慧眼卫星是时序数据，可以按时间窗口分片。一个分片内的数据，使用位图BitMap对数据进行索引。EventDB接收的事例数据流可以是批处理中的文件流，也可以是实时系统中的socket包。高能物理的实验数据一般以文件的方式存储和管理，例如高能粒子对撞实验数据保存在ROOT格式的文件中，天文卫星实验数据保存在FITS格式的文件中。为了使EventDB具有普适性，输入模块对各种事例输入流做了抽象。  
事例对象抽象为event\<t, p1, p2, ..., pn\>， 其中t是event分片属性，p1~pn是事例的其它属性，事例输入流抽象为List\<even>，事例event所属的分片bucketId = getBucketId(event.t)。一个分片内的事例数据和位图索引，构成了一个临时的bucket table, 所有的bucket table作为HBase的输入流，最后将事例数据和位图索引存储在HBase中。  
EventDB根据模块功能可以抽象为三层:

1. 索引层  
	索引层接收事例数据流，完成对各种数据流的抽象，并对事例数据进行切片，最后通过位图BitMap对分片内的数据进行索引，形成bucket table作为存储层的输入。
	<img src="https://github.com/AshinGau/eventdb-java/blob/master/pics/MDIndex.PNG?raw=true" width="600" align=center />  

2. 存储层  
	存储层接收索引层输出的bucket table。bucket table的第一行是原始的事例数据，为了存储在HBase中，需要对List\<event\>进行序列化处理。序列化不是本论文的研究点，可以采用通用的序列化工具如google的protobuf对List\<event\>进行序列化处理。bucket table的其它行是满足event.px = valx事例的位图索引。把bucket table直接写入HBase存在两个问题：1)bucket table的原始数据和位图索引很有可能不在一个region上，导致得到位图索引以后需要跨region读取原始数据；2)bucket table很有可能导致HBase产生数据倾斜，有大量的数据往一个region中写入，该region就会不断的分裂，影响写入速度。EventDB采用预挂载region，并通过一致性Hash把bucket table映射到region上。这样存在一个问题，当某个预挂载的region满了以后，即使在Hash环上继续挂载新的region，依然会有bucket table映射到该region上，导致该region分裂。为了解决这个问题，Hash环上的Node不和region进行绑定，而是通过元信息进行映射。HBase中有一个Region.0用来记录元信息，Region.0中保存了\<node.x, region.x\>的映射关系，当region.x满了以后，HBase新生成一个Region.n+1，并更新node.x的映射关系为\<node.x, region.n+1\>。这样又带来了一个问题，由于node.x只记录了当前的映射关系，region.x中的bucket table无法通过Hash环获取到，查询的时候无法根据Hash环找到bucketId存储的region。所以，元信息region.0还需要记录\<bucketId, region\>的映射关系。把bucket table写入region.x很简单，只需要把region.x的startKey做为RowKey的前缀。  
	<img src="https://github.com/AshinGau/eventdb-java/blob/master/pics/ConsistentHash.PNG?raw=true" width="600" align=center />  

3. 查询层