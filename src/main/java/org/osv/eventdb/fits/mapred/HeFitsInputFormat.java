package org.osv.eventdb.fits.mapred;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
                                                                                                                                                                                                                     
public class HeFitsInputFormat extends FileInputFormat<LongWritable, HeEvt>{
    @Override
    public RecordReader<LongWritable, HeEvt>
        createRecordReader(InputSplit split,
                TaskAttemptContext context){
            return new HeEvtRecordReader();
        }   
    @Override
    protected boolean isSplitable(JobContext context, Path file){
        return true;
    }   
}
