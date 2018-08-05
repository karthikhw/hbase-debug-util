package com.hbase.util;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class Mapper extends org.apache.hadoop.mapreduce.Mapper<Text, Text, Text, Text>{
	
	
	
@Override
protected void setup(org.apache.hadoop.mapreduce.Mapper<Text, Text, Text, Text>.Context context)
		throws IOException, InterruptedException {

}

@Override
	protected void map(Text key, Text value, org.apache.hadoop.mapreduce.Mapper<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
	
	}


}
