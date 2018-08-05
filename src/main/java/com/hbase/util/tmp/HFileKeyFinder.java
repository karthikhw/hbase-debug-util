package com.hbase.util.tmp;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFile.Reader;
import org.apache.hadoop.hbase.util.Bytes;

public class HFileKeyFinder {
	public static void main(String args[]) throws IOException {
		HFile hFile = new HFile();
		Configuration conf = HBaseConfiguration.create();
		FileSystem fileSystem = FileSystem.get(conf);
		RemoteIterator<LocatedFileStatus> list = fileSystem.listFiles(new Path(args[0]), true);
		StringBuffer keyList = new StringBuffer();
		while (list.hasNext()) {
			LocatedFileStatus lfs = list.next();
			Path path = lfs.getPath();
			if (hFile.isHFileFormat(fileSystem, path)) {
				Reader reader = hFile.createReader(fileSystem, path, new CacheConfig(conf), conf);
				reader.loadFileInfo();
				String sKey = Bytes.toStringBinary(reader.getFirstRowKey());
				String eKey = Bytes.toStringBinary(reader.getLastRowKey());
				keyList.append(path.toString() + "  " + sKey + "  " + eKey + "\n");
			} else {
				System.out.println("Skipping file " + path.toString() + " Not in HFile format.");
			}
		}
		System.out.println("==================");
		System.out.println(keyList.toString());
		System.out.println("==================");
	}
}
