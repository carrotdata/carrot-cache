package com.onecache.core.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDecompressCtx;
import com.github.luben.zstd.ZstdDictCompress;
import com.github.luben.zstd.ZstdDictDecompress;
import com.github.luben.zstd.ZstdDictTrainer;

public class TestZstdCompressionText {

  private static int DICT_SIZE = 1 << 20; // 16KB
  private static int COMP_LEVEL = 3;
  
  @SuppressWarnings("unused")
  public static void main(String[] args) throws IOException {
    String file = "/Users/vrodionov/Downloads/silesia/dickens";
    
    // Load all files
     int totalSize = 0;
     byte[] b = Files.readAllBytes(Path.of(file));
     totalSize += b.length;
     List<byte[]> trainingList = new ArrayList<byte[]>();
     trainingList = getList(b);
     
     System.out.printf("Avg sentence length=%d\n", totalSize / trainingList.size());
     
     ZstdDictTrainer trainer = new ZstdDictTrainer(totalSize, DICT_SIZE);
     List<byte[]> toTrain = trainingList.subList(0, trainingList.size());
     for (byte[] bb: toTrain) {
       trainer.addSample(bb);
     }
     long start = System.currentTimeMillis();
     ByteBuffer dictData = trainer.trainSamplesDirect();
     long end = System.currentTimeMillis();
     
     System.out.printf("Training time %d sample size=%d\n", end - start, totalSize);
     ZstdDictCompress dictCompress = new ZstdDictCompress(dictData, COMP_LEVEL);
     
     ZstdCompressCtx compContext = new ZstdCompressCtx();
     compContext.loadDict(dictCompress);
     compContext.setLevel(COMP_LEVEL);

     int n = 80;
     System.out.printf("Group of:%d\n", n);
     List<byte[]> group = groupOf(trainingList, n);
     //group = group.subList(group.size()/2, group.size());
     
     List<byte[]> compresed = compress(compContext, group);
     
     compresed = compressNativeNative(compContext, group);
     
     compresed = compressNativeByteArray(compContext, group);
     
     compresed = compressByteArrayNative(compContext, group);

     
     List<Integer> sizes = group.stream().map(x -> x.length).collect(Collectors.toList());
     
     ZstdDictDecompress dictDecompress = new ZstdDictDecompress(dictData);
     ZstdDecompressCtx decompContext = new ZstdDecompressCtx();
     decompContext.loadDict(dictDecompress);
     List<byte[]> decompressed = decompress(decompContext, compresed, sizes);
     
     decompressNativeNative(decompContext, compresed, sizes);
     
     decompressNativeByteArray(decompContext, compresed, sizes);
     
     decompressByteArrayNative(decompContext, compresed, sizes);

     byte[] buf = new byte[1<< 16];
     decompress(decompContext, compresed, sizes, buf);

     compContext.close();
     decompContext.close();
  }
  
  private static List<byte[]> getList(byte[] b) {
    String s = new String(b);
    int i = s.indexOf("CHAPTER");
    s = s.substring(i);
    s = s.replaceAll("\n|\r", " ");

    String[] parts = s.split("\\.|\\?|\\!");
    List<String> list = Arrays.stream(parts).filter(x -> x.length() > 0).collect(Collectors.toList());
    list.stream().forEach(x -> x.trim());
//    for (int j = 0; j < 100; j++) {
//      System.out.println(list.get(j));
//    }
    return list.stream().map( x-> x.getBytes()).collect(Collectors.toList());
  }

  private static List<byte[]> compress(ZstdCompressCtx context, List<byte[]> source){
    
    ArrayList<byte[]> result = new ArrayList<byte[]>(source.size());
    long start = System.nanoTime();
    long totalSize = 0;
    long compSize = 0;
    for(byte[] b: source) {
      byte[] cb = context.compress(b);
      result.add(cb);
      totalSize += b.length;
      compSize += cb.length;
    }
    long end = System.nanoTime();
    System.out.printf("Compression time %d micros total samples=%d\n", (end - start) / 1000, source.size());
    System.out.printf("Total size=%d compressed=%d ratio=%f\n", totalSize, compSize, (double) totalSize/ compSize);
    System.out.printf("Compression speed=%fMB/s\n", ((double) totalSize * 1000000000) / ((1L << 20) * (end - start)));
    return result;
  }
  
 private static List<byte[]> compressNativeNative(ZstdCompressCtx context, List<byte[]> source){
    
    ArrayList<byte[]> result = new ArrayList<byte[]>(source.size());
    long totalSize = 0;
    long compSize = 0;
    long src = UnsafeAccess.malloc(100000);
    long dst = UnsafeAccess.malloc(100000);
    long start = System.nanoTime();
    long total = 0;
    for(byte[] b: source) {
      UnsafeAccess.copy(b, 0, src, b.length);
      long t1 = System.nanoTime();
      int size = context.compressNativeNative(dst, b.length + 100, src, b.length);
      long t2 = System.nanoTime();
      total += t2 - t1;
      byte[] cb = new byte[size];
      UnsafeAccess.copy(dst, cb, 0, size);
      result.add(cb);
      totalSize += b.length;
      compSize += cb.length;
      
    }
    UnsafeAccess.free(src);
    UnsafeAccess.free(dst);
    long end = System.nanoTime();
    System.out.printf("Native-Native Compression time %d micros total samples=%d raw time=%d\n", (end - start) / 1000, source.size(), total);
    System.out.printf("Total size=%d compressed=%d ratio=%f\n", totalSize, compSize, (double) totalSize/ compSize);
    System.out.printf("Compression speed=%fMB/s\n", ((double) totalSize * 1000000000) / ((1L << 20) * (end - start)));

    return result;
  }
  
 private static List<byte[]> compressNativeByteArray(ZstdCompressCtx context, List<byte[]> source){
   
   ArrayList<byte[]> result = new ArrayList<byte[]>(source.size());
   long totalSize = 0;
   long compSize = 0;
   long src = UnsafeAccess.malloc(100000);
   byte[] dst = new byte[100000];
   long start = System.nanoTime();

   for(byte[] b: source) {
     UnsafeAccess.copy(b, 0, src, b.length);
     int size = context.compressNativeByteArray(dst, 0, b.length + 100, src, b.length);
     
     byte[] cb = new byte[size];
     System.arraycopy(dst, 0, cb, 0, size);
     result.add(cb);
     totalSize += b.length;
     compSize += cb.length;
     
   }
   UnsafeAccess.free(src);
   long end = System.nanoTime();
   System.out.printf("Native-ByteArray Compression time %d micros total samples=%d\n", (end - start) / 1000, source.size());
   System.out.printf("Total size=%d compressed=%d ratio=%f\n", totalSize, compSize, (double) totalSize/ compSize);
   System.out.printf("Compression speed=%fMB/s\n", ((double) totalSize * 1000000000) / ((1L << 20) * (end - start)));

   return result;
 }
 
 
 private static List<byte[]> compressByteArrayNative(ZstdCompressCtx context, List<byte[]> source){
   
   ArrayList<byte[]> result = new ArrayList<byte[]>(source.size());
   long totalSize = 0;
   long compSize = 0;
   long dst = UnsafeAccess.malloc(100000);
   //byte[] src = new byte[100000];
   long start = System.nanoTime();

   for(byte[] b: source) {
     int size = context.compressByteArrayNative(dst,  b.length + 100, b, 0, b.length);
     
     byte[] cb = new byte[size];
     UnsafeAccess.copy(dst, cb, 0, size);
     result.add(cb);
     totalSize += b.length;
     compSize += cb.length;
     
   }
   UnsafeAccess.free(dst);
   long end = System.nanoTime();
   System.out.printf("ByteArray-Native Compression time %d micros total samples=%d\n", (end - start) / 1000, source.size());
   System.out.printf("Total size=%d compressed=%d ratio=%f\n", totalSize, compSize, (double) totalSize/ compSize);
   System.out.printf("Compression speed=%fMB/s\n", ((double) totalSize * 1000000000) / ((1L << 20) * (end - start)));

   return result;
 }
 
  private static List<byte[]> decompress(ZstdDecompressCtx context, List<byte[]> compressed, List<Integer> sizes){
    ArrayList<byte[]> result = new ArrayList<byte[]>(compressed.size());
    int i = 0;
    long decompSize = 0;
    long start = System.nanoTime();

    for(byte[] b: compressed) {
      byte[] db = context.decompress(b, sizes.get(i++));
      decompSize += db.length;
      result.add(db);
    }
    long end = System.nanoTime();
    System.out.printf("Decompression time=%d size=%d speed=%f MB/s\n", (end - start) / 1000,  decompSize,
      ((double) decompSize * 1000000000) / ((1L << 20) * (end - start)));

    return result;
  }
  
  
  private static List<byte[]> decompressNativeNative(ZstdDecompressCtx context, List<byte[]> compressed, List<Integer> sizes){
    ArrayList<byte[]> result = new ArrayList<byte[]>(compressed.size());
    long decompSize = 0;
    long src = UnsafeAccess.malloc(100000);
    long dst = UnsafeAccess.malloc(100000);

    long start = System.nanoTime();
    long total = 0;
    for(byte[] b: compressed) {
      UnsafeAccess.copy(b, 0, src, b.length);
      long t1 = System.nanoTime();
      int size = context.decompressNativeNative(dst, 100000, src, b.length);
      long t2 = System.nanoTime();
      total+= t2 - t1;
      byte[] db = new byte[size];
      UnsafeAccess.copy(dst, db,  0, size);
      decompSize += db.length;
      result.add(db);
    }
    long end = System.nanoTime();
    System.out.printf("Decompression  Native-Native time=%d size=%d speed=%f MB/s raw time=%d\n", (end - start) / 1000,  decompSize,
      ((double) decompSize * 1000000000) / ((1L << 20) * (end - start)), total);

    UnsafeAccess.free(src);
    UnsafeAccess.free(dst);
    return result;
  }
  
  private static List<byte[]> decompressNativeByteArray(ZstdDecompressCtx context, List<byte[]> compressed, List<Integer> sizes){
    ArrayList<byte[]> result = new ArrayList<byte[]>(compressed.size());
    long decompSize = 0;
    long src = UnsafeAccess.malloc(100000);
    byte[] dst = new byte[100000];

    long start = System.nanoTime();

    for(byte[] b: compressed) {
      UnsafeAccess.copy(b, 0, src, b.length);
      
      int size = context.decompressNativeByteArray(dst, 0, 100000, src, b.length);
      byte[] db = new byte[size];
      System.arraycopy(dst, 0,  db, 0, size);
      decompSize += db.length;
      result.add(db);
    }
    long end = System.nanoTime();
    System.out.printf("Decompression  Native-ByteArray time=%d size=%d speed=%f MB/s\n", (end - start) / 1000,  decompSize,
      ((double) decompSize * 1000000000) / ((1L << 20) * (end - start)));

    UnsafeAccess.free(src);
    return result;
  }
  
  private static List<byte[]> decompressByteArrayNative(ZstdDecompressCtx context, List<byte[]> compressed, List<Integer> sizes){
    ArrayList<byte[]> result = new ArrayList<byte[]>(compressed.size());
    long decompSize = 0;
    long src = UnsafeAccess.malloc(100000);
    long dst = UnsafeAccess.malloc(100000);

    long start = System.nanoTime();

    for(byte[] b: compressed) {
      UnsafeAccess.copy(b, 0, src, b.length);
      
      int size = context.decompressByteArrayNative(dst, 100000, b, 0, b.length);
      byte[] db = new byte[size];
      UnsafeAccess.copy(dst, db, 0, size);
      decompSize += db.length;
      result.add(db);
    }
    long end = System.nanoTime();
    System.out.printf("Decompression  ByteArray-Native time=%d size=%d speed=%f MB/s\n", (end - start) / 1000,  decompSize,
      ((double) decompSize * 1000000000) / ((1L << 20) * (end - start)));

    UnsafeAccess.free(src);
    return result;
  }
  
  private static void decompress(ZstdDecompressCtx context, List<byte[]> compressed, List<Integer> sizes, byte[] buf){
    int i = 0;
    long decompSize = 0;
    long start = System.nanoTime();

    for(byte[] b: compressed) {
      
      int size = context.decompressByteArray(buf, 0, sizes.get(i++), b, 0, b.length);
      decompSize += size;
    }
    long end = System.nanoTime();
    System.out.printf("Decompression ByteArraytime=%d size=%d speed=%f MB/s\n", (end - start) / 1000,  decompSize,
      ((double) decompSize * 1000000000) / ((1 << 20) * (end - start)));

  }
  
  private static byte[] next(List<byte[]> list, int start, int count) {
    
    if(start == list.size()) {
      return null;
    }
    count = Math.min(count, list.size() - start);
    int size = 0;
    for (int i = start; i < start + count; i++) {
      size += list.get(i).length;
    }
    byte[] buf = new byte[size];
    int off = 0;
    for (int i = start; i < start + count; i++) {
      byte[] b = list.get(i);
      System.arraycopy(b, 0, buf, off, b.length);
      off += b.length;
    }
    return buf;
  }
  
  private static List<byte[]> groupOf(List<byte[]> source, int count){
    ArrayList<byte[]> result = new ArrayList<byte[]>(source.size() / count * count + 1);
    int index = 0;
    while(true) {
      count = Math.min(count, source.size() - index);
      byte[] b = next(source, index, count);
      if (b == null) {
        break;
      }
      result.add(b);
      index += count;
    }
    return result;
  }
}
