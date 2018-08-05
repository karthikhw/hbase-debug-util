package com.hbase.util.tmp;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class Write {

	public static void main(String[] args) throws IOException {
			  HTable htable = null;
		        try {
		            Configuration conf = HBaseConfiguration.create();
					htable = new HTable(conf, args[0]);
		            Put put = null;
					for (int i = 0; i < 1000; i++) {
						put = new Put(Bytes.toBytes(i));
						put.add(Bytes.toBytes("d"), Bytes.toBytes("c"), Bytes.toBytes(i));
						htable.put(put);
					}
		        }
		        finally {
		            if (htable != null) {
		                htable.close();
		            }
		        }

	
	}

}
