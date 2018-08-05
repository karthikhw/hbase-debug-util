package com.hbase.util;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Driver {

	private static Configuration conf = null;
	private static final String OPTZOOKEEPERSERVER = "zookeeper-server";
	private static final String OPTZOOKEEPERPORT = "zookeeper-port";
	private static final String OPTZOOKEEPERZNODE = "zookeeper-znode";
	private static final String OPTTABLENAME = "table-name";
	private static final String OUTPUTDIRECTORY = "output-dir";


	public void setConf(CommandLine cmdline) {
		if (cmdline.hasOption("z")) {
			conf.set(HConstants.ZOOKEEPER_QUORUM, cmdline.getOptionValue(OPTZOOKEEPERSERVER));
		}
		if (cmdline.hasOption("p")) {
			conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, cmdline.getOptionValue(OPTZOOKEEPERPORT));
		}
		if (cmdline.hasOption("n")) {
			conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, cmdline.getOptionValue(OPTZOOKEEPERZNODE));
		}
		if (cmdline.hasOption("t")) {
			conf.set(TableInputFormat.INPUT_TABLE, cmdline.getOptionValue(OPTTABLENAME));
		}
	}


	public static void usage(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp(
				"java -cp `hbase classpath` com.hbase.util.Driver [--zookpeer-server <comma seperated list>] [--zookeper-znode <znode>] [--zookeeper-port <zk port>] --table-name <tablename> --output-dir <dirname>  2>/tmp/_progress.log\n",
				options);
		System.exit(-1);
	}
	
	  static String convertScanToString(Scan scan) throws IOException {
		    ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
		    return Base64.encodeBytes(proto.toByteArray());
		  }

	public static void main(String[] args) {
		Options options = new Options();
		try {
			Driver status = new Driver();
			CommandLineParser parser = new GnuParser();
			options.addOption("z", OPTZOOKEEPERSERVER, true, "comma seperated zookeeper servers");
			options.addOption("p", OPTZOOKEEPERPORT, true, "zookeeper port");
			options.addOption("n", OPTZOOKEEPERZNODE, true, "zookeeper znode");
			options.addOption("t", OPTTABLENAME, true, "table name");
			options.addOption("o", OUTPUTDIRECTORY, true, "output directory");
			CommandLine line = parser.parse(options, args);
			conf = HBaseConfiguration.create();
			status.setConf(line);
			Scan scan = new Scan();
			conf.set("hbase.mapreduce.scan", convertScanToString(scan));
			Job job = new org.apache.hadoop.mapred.jobcontrol.Job(new JobConf(conf))
			job.setJarByClass(Driver.class);
			job.setMapperClass();
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
		    job.setOutputFormatClass(TextOutputFormat.class);
			job.setInputFormatClass(TableInputFormat.class);
			FileOutputFormat.setOutputPath(job, new org.apache.hadoop.fs.Path(line.getOptionValue(OUTPUTDIRECTORY)));
			job.waitForCompletion(true);
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
}