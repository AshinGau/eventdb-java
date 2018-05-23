package org.osv.eventdb.fits.mapred;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class HeEvtRecordReader extends RecordReader<LongWritable, HeEvt>{
	private long start;
	private long pos;
	private long end;
	private int evtWidth;
	private LongWritable key;
	private HeEvt value;
	private FSDataInputStream fileIn;
	public HeEvtRecordReader(){
		evtWidth = 16;
	}
	public HeEvtRecordReader(int evtWidth){
		this.evtWidth = evtWidth;
	}
	@Override
	public void initialize(InputSplit genericSplit,
			TaskAttemptContext context) throws IOException, InterruptedException{
		FileSplit split = (FileSplit)genericSplit;
		Configuration job = context.getConfiguration();
		start = split.getStart();
		end = start + split.getLength();
		final Path file = split.getPath();

		//open the file and seek to the start of the split
		final FileSystem fs = file.getFileSystem(job);
		fileIn = fs.open(file);
		fileIn.seek(start);
		pos = start;

		key = new LongWritable();
		value = new HeEvt();
	}
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException{
		if(pos < end){
			key.set(pos);
			fileIn.read(value.getEvt());
			pos += evtWidth;
			return true;
		}
		return false;
	}
	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException{
		return key;
	}
	@Override
	public HeEvt getCurrentValue() throws IOException, InterruptedException{
		return value;
	}
	@Override
	public float getProgress() throws IOException, InterruptedException{
		return Math.min(1.0f, (pos - start) / (float)(end - start));
	}
	@Override
	public void close() throws IOException{
		fileIn.close();
	}
}
