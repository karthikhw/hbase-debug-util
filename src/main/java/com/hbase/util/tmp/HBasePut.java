package com.hbase.util.tmp;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class HBasePut {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, args[0]);
		Put put = null;
		for (int i = 0; i < Integer.parseInt(args[1]); i++) {
		put = new Put(Bytes.toBytes(i));	
		put.add(Bytes.toBytes("d"), Bytes.toBytes("c"),Bytes.toBytes("DataBlock"));
		table.put(put);
		Thread.sleep(Long.parseLong(args[2]));
		}
	}

}
