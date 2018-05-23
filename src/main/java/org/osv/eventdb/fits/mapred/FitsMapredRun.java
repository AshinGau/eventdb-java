package org.osv.eventdb.fits.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.client.Put;

public class FitsMapredRun {
	public static void formatHeFits(String fitsDir, String dstHdfsDir) throws Exception {
		HeFitsFormat.formatDir(fitsDir, dstHdfsDir);
	}

	public static void mapredHeFits(String inputPath, String tableName) throws Exception{
		String sqPath = "/fits/workspace/sq";
		String hfPath = "/fits/workspace/hf";

		Configuration conf = new Configuration();
		Job job1 = Job.getInstance(conf, "HeFits2SequenceFile");
		job1.setJarByClass(FitsMapredRun.class);
		job1.setMapperClass(HeFitsMapper1.class);
		job1.setReducerClass(HeFitsReducer1.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(HeEvtData.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(HeEvtArray.class);
		job1.setInputFormatClass(HeFitsInputFormat.class);
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);

		//SequenceFileOutputFormat.setOutputCompressionType(job1, CompressionType.NONE);
		SequenceFileOutputFormat.setOutputCompressionType(job1, CompressionType.RECORD);
		SequenceFileOutputFormat.setOutputCompressorClass(job1, DefaultCodec.class);

		HeFitsInputFormat.addInputPath(job1, new Path(inputPath));
		SequenceFileOutputFormat.setOutputPath(job1, new Path(sqPath));

		if (job1.waitForCompletion(true)) {
			Configuration hconf = HBaseConfiguration.create();
			hconf.set("hbase.zookeeper.property.clientPort", "2181");
			hconf.set("hbase.zookeeper.quorum", "sbd03,sbd05.ihep.ac.cn,sbd07.ihep.ac.cn");
			Job job2 = Job.getInstance(hconf, "HeFits2HFile");
			job2.setJarByClass(FitsMapredRun.class);
			job2.setMapperClass(HeFitsMapper2.class);
			job2.setMapOutputKeyClass(ImmutableBytesWritable.class);
			job2.setMapOutputValueClass(Put.class);
			// speculation  
			job2.setSpeculativeExecution(false);
			job2.setReduceSpeculativeExecution(false);
			// in/out format  
			job2.setInputFormatClass(SequenceFileInputFormat.class);
			job2.setOutputFormatClass(HFileOutputFormat2.class);

			SequenceFileInputFormat.setInputPaths(job2, sqPath);
			FileOutputFormat.setOutputPath(job2, new Path(hfPath));

			Connection connection = ConnectionFactory.createConnection(hconf);
			Table hTable = connection.getTable(TableName.valueOf(tableName));
			//Admin admin = connection.getAdmin();
			HFileOutputFormat2.configureIncrementalLoad(job2, hTable,
					connection.getRegionLocator(TableName.valueOf(tableName)));
			if (job2.waitForCompletion(true)) {
				//LoadIncrementalHFiles load = new LoadIncrementalHFiles(hconf);
				//load.doBulkLoad(new Path(hfPath), admin, hTable,connection.getRegionLocator(TableName.valueOf(tableName)));
				System.exit(0);
			}
		}
		System.exit(1);
	}
}
