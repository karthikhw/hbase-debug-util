package com.hbase.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

public class Replication {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		Configuration peerConf = new Configuration();
		   peerConf.addResource(new Path("/etc/hbase/repl/hbase-site.xml"));
	        peerConf.addResource(new Path("/etc/hbase/repl/core-site.xml"));
	        peerConf.addResource(new Path("/etc/hbase/repl/hdfs-site.xml"));
	        System.setProperty("java.security.krb5.conf", "/etc/hbase/repl/krb5.conf");
	        UserGroupInformation.setConfiguration(peerConf);
			try {
				UserGroupInformation.loginUserFromKeytab("hbase-hwx263@HWX.COM","/etc/hbase/repl/hbase.headless.keytab");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	        
			HTable table = new HTable(peerConf, Bytes.toBytes("t1"));
			Result rs = table.get(new Get(Bytes.toBytes("karthik")));
			System.out.println(Bytes.toStringBinary(rs.getRow()));
	}
}
