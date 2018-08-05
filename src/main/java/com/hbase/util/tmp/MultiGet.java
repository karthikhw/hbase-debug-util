package com.hbase.util.tmp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class MultiGetTest {

	public static void main(String[] args) throws IOException, InterruptedException {
		
			Configuration conf = HBaseConfiguration.create();
			HTable table = new HTable(conf, args[1]);
String command = args[0];
int size = Integer.parseInt(args[2]);
			if (command.equalsIgnoreCase("get")){
				List<Get> getList = new ArrayList<Get>();
				Get get = null;
				for (int i = 0; i < size; i++) {
					 get = new Get(Bytes.toBytes(i));
					 get.addColumn(Bytes.toBytes("d"), Bytes.toBytes("c"));
					 getList.add(get);
				}
				Result[] rs = table.get(getList);
				for (int j = 0; j < rs.length; j++) {
						System.out.println("Get "+ Bytes.toStringBinary(rs[j].getRow()));
				}
			}else if(command.equalsIgnoreCase("put")){
				Put put = null;
				for (int i = 0; i < size; i++) {
				put = new Put(Bytes.toBytes(i));
				put.add(Bytes.toBytes("d"), Bytes.toBytes("c"), Bytes.toBytes(i));
				table.put(put);
				}
			}else {
				System.out.println("Invalid command");
			}
			
			table.close();
	}
}
