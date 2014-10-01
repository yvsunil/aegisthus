package com.netflix.aegisthus.tools;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;

import org.xerial.snappy.SnappyInputStream;

public class Decompress {
  public static void main(String[] args) throws Exception {
    String base = "/Users/daniel/base/coursera/aegisthus/data4/";
    decompressAndClose(base + "assess-kvs_strict-ic-2770-Index.db", base + "assess-kvs_strict-ic-2770-Index2.db");
  }
  
  public static void decompressAndClose(String inName, String outName) throws Exception {
      	int BUFFER = 4 * 1024;

	   SnappyInputStream is = new SnappyInputStream(new BufferedInputStream(new FileInputStream(inName)));
	   
	   OutputStream out = new FileOutputStream(outName);
	   BufferedOutputStream dest = new BufferedOutputStream(out, BUFFER);
	   
	   int c;
	   byte data[] = new byte[BUFFER];
	   while ((c = is.read(data, 0, BUFFER)) != -1) {
	       dest.write(data, 0, c);
	   }
	   
	   is.close();
	   dest.close();
  }
}
