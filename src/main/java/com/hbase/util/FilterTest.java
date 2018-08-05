package com.hbase.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

public class FilterTest {

		
			public static void main(String args[])throws IOException {
			Configuration conf = HBaseConfiguration.create();
			HTable table = new HTable(conf,"customer");
			Scan scan = new Scan();
			scan.addColumn(Bytes.toBytes("order"),Bytes.toBytes("number"));
			Filter filter1 = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("Fli"));
			FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE,filter1);
			scan.setFilter(list);
			ResultScanner result = table.getScanner(scan);
			for(Result res:result){
			byte[] val = res.getValue(Bytes.toBytes("order"), Bytes.toBytes("number"));
			System.out.println("Row-value : "+Bytes.toString(val));
			System.out.println(res);
			}
			table.close();
			}

}
