package com.hbase.util.tmp;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.ClientScanner;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseScan {

	public static void main(String[] args) throws IOException {
		
		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, args[0]);
        Scan scan = new Scan();
        scan.setTimeRange(Long.valueOf(args[1]), Long.valueOf(args[2]));
        ResultScanner rs = table.getScanner(scan);
        Result result = null;
        long count = -1;
        ClientScanner scanner = (ClientScanner) rs;
        while((result=scanner.next())!=null){
        	//System.out.println(Bytes.toStringBinary(result.getRow()));
        	scanner.renewLease();
        	count=count+1;
        }
        System.out.println("Scanner next count"+count);
	}
}
