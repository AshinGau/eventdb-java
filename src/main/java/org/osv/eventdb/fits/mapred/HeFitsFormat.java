package org.osv.eventdb.fits.mapred;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.conf.Configuration;

public class HeFitsFormat{
	FileInputStream fin;
	FSDataOutputStream fout;
	long rowCount;
	public HeFitsFormat(String local_file, String hdfs_uri, String hdfs_file, String user)
		throws Exception{
		Configuration config = new Configuration();
		config.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		FileSystem hdfs = FileSystem.get(new URI(hdfs_uri), config, user);
		format(hdfs, local_file, hdfs_file);
	}
	public HeFitsFormat(String local_file, String hdfs_uri, String hdfs_file)
		throws Exception{
		this(local_file, hdfs_uri, hdfs_file, "root");
	}
	public HeFitsFormat(String local_file, String hdfs_file)
		throws Exception{
		this(local_file, "hdfs://192.168.60.64", hdfs_file, "root");
	}
	public HeFitsFormat(FileSystem hdfs, String local_file, String hdfs_file)
		throws Exception{
		format(hdfs, local_file, hdfs_file);
	}
	void format(FileSystem hdfs, String local_file, String hdfs_file)
		throws Exception{
		fout = hdfs.create(new Path(hdfs_file));
		fin = new FileInputStream(local_file);
		fin.skip(2880 * 2 + 80 * 4 + 9);
		byte[] brows = new byte[22];
		fin.read(brows);
		String rows = new String(brows);
		rowCount = Long.valueOf(rows.trim());
		//test
		//rowCount = 500000;

		//format to hdfs
		fin.skip(2880 * 4 - 2880 * 2 - 80 * 4 - 9 - 22);
		long total = 16 * rowCount;
		int flush_size = 1024 * 10;
		byte[] flush = new byte[flush_size];
		long len = 0;
		while(len < total){
			len += flush_size;
			if(len <= total){
				fin.read(flush);
				fout.write(flush, 0, flush_size);
			}else{
				int remain = (int)(total - len + flush_size);
				fin.read(flush, 0, remain);
				fout.write(flush, 0, remain);
			}
		}
		close();
		System.out.printf("fits: %s has been formatted\n", local_file);
	}
	void close()
		throws Exception{
		fin.close();
		fout.close();
	}
	public long getRowCount(){
		return rowCount;
	}
	public static void formatDir(String local_dir, String hdfs_dir) throws Exception{
		Configuration config = new Configuration();
		config.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		FileSystem hdfs = FileSystem.get(new URI("hdfs://192.168.60.64"), config, "root");

		File localDir = new File(local_dir);
		File[] lists = localDir.listFiles();
		for(File fits: lists){
			String local_file = fits.getAbsolutePath();
			String local_file_name = fits.getName();
			Path hdfs_path = new Path(hdfs_dir, local_file_name + ".evt");
			new HeFitsFormat(hdfs, local_file, hdfs_path.toString());
		}
	}
}
