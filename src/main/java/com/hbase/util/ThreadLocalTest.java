package com.hbase.util;

import java.io.IOException;

public class ThreadLocalTest {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
	ThreadLocal<String> local = new ThreadLocal<String>();
	local.set("kp");
	System.out.println(local.get());
	System.out.println(local.get());
	
		

	}

}
