package com.carrotdata.cache.compression.zstd;

import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.carrotdata.cache.util.UnsafeAccess;
import com.github.luben.zstd.Zstd;

public class TestZstdCompressionCodec {

  ZstdCompressionCodec codec;
  
  @Before
  public void setUp() throws Exception {
    codec = new ZstdCompressionCodec();
    codec.setCacheName("testCache");
  }
  
  @Test
  public void testCodec() {
    // Add test cases for ZstdCompressionCodec here
    
    String testString = "This is a test string for Zstd compression codec.";
    testString = testString.repeat(10);
    
    try {
      byte[] inputData = testString.getBytes("UTF-8");
      
      // Compress the data
      byte[] out = new byte[testString.length() * 2];
      long ptr = UnsafeAccess.malloc(out.length);
      UnsafeAccess.copy(inputData, 0, ptr, inputData.length);      
      int compSize = codec.compress(ptr, inputData.length, 0);
      byte[] compData = new byte[compSize];
      UnsafeAccess.copy(ptr, compData, 0, compSize);
      byte[] decom = Zstd.decompress(compData, inputData.length);
      
      System.out.println("Original Size: " + inputData.length);
      System.out.println("Compressed Size: " + compSize);
      System.out.println("Decompressed Size Zstd: " + decom.length);
      assertTrue(decom.length == inputData.length);
      String decomResult = new String(decom, "UTF-8");
      assertTrue(testString.equals(decomResult));
      
      int decompSize = codec.decompress(ptr, compSize, out, 0);
      System.out.println("Decompressed Size: " + decompSize);
      assertTrue(decompSize == inputData.length);
      String result = new String(out, 0, decompSize, "UTF-8");
      assertTrue(testString.equals(result));
      
      
    } catch (Exception e) {
      e.printStackTrace();
      assert false : "Exception occurred during ZstdCompressionCodec test.";
    }
    
  }
  
  @After
  public void tearDown() throws Exception {
    codec = null;
  }
}
