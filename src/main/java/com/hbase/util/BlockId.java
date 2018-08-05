package com.hbase.util;

public class BlockId {
	  private static final String SEP = System.getProperty("file.separator");

	public static void main(String[] args) {
		long blockId = 1073768027L;
		int d1 = (int) ((blockId >> 16)); 
		int d2 = (int) ((blockId >> 8)); 
		String path = "subdir" + d1 + SEP + 
		"subdir" + d2; 
	System.out.println(4194406 % 256);

	}

}
