# eventdb
> a multidimensional database system for semi-strucured big data.

Like most big data systems, eventdb uses fragmentation technology to store and retrieve data. For the robustness and extensibility of the system, eventdb is built on HBase in a non-invasive manner, which means that you can store and retrieve data through original API like java client, thrift .etc.

## Install & Run
1. mvn package
2. hadoop fs -put target/eventdb-1.0.0.jar /hdfs/path/to/store/java-jar/
3. java -jar target/eventdb-1.0.0.jar createTable tableName initialSplitNumber
4. java -jar target/eventdb-1.0.0.jar observer tableName org.osv.eventdb.fits.FitsObserver /hdfs/path/to/store/java-jar/eventdb-1.0.0.jar
5. java -jar target/eventdb-1.0.0.jar insertHeFits /path/of/fitsfile tableName

## Semi-structured data storage
In a traditional way, a  record of data may store in a single row and each attrribute is stored as a cell, which produces a long table in the big data envirnment and wasts a lot of index storage. Eventdb stores the whole data of a fragmentation in a cell in a single row, and writes the serialized byte stream of these datas as storage. We make a convention that the term **event** denotes a record of the semi-structured data, that's why the system is called eventdb. How to serialize & deserialize of the byte stream of events in a fragementation can be customed by the specific project you participate in.
I'd like to introduce an uncomplicated and intuitive method
