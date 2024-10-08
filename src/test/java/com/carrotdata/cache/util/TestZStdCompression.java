/*
 * Copyright (C) 2024-present Carrot Data, Inc. 
 * <p>This program is free software: you can redistribute it
 * and/or modify it under the terms of the Server Side Public License, version 1, as published by
 * MongoDB, Inc.
 * <p>This program is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE. See the Server Side Public License for more details. 
 * <p>You should have received a copy of the Server Side Public License along with this program. If not, see
 * <http://www.mongodb.com/licensing/server-side-public-license>.
 */
package com.carrotdata.cache.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDecompressCtx;
import com.github.luben.zstd.ZstdDictCompress;
import com.github.luben.zstd.ZstdDictDecompress;
import com.github.luben.zstd.ZstdDictTrainer;

public class TestZStdCompression {
  private static final Logger LOG = LoggerFactory.getLogger(TestZStdCompression.class);

  private static int DICT_SIZE = 1 << 20; // 16KB
  private static int COMP_LEVEL = 3;

  public static void main(String[] args) throws IOException {
    String dir = "/Users/vrodionov/Development/datasets/github";
    // Load all files
    List<Path> fileList = Files.list(Path.of(dir)).collect(Collectors.toList());
    List<byte[]> trainingList = new ArrayList<byte[]>();
    int totalSize = 0;
    for (Path p : fileList) {
      byte[] b = Files.readAllBytes(p);
      totalSize += b.length;
      trainingList.add(b);
    }
    LOG.info("Total size={} avg={}", totalSize, (float) totalSize / fileList.size());
    ZstdDictTrainer trainer = new ZstdDictTrainer(totalSize, DICT_SIZE);
    List<byte[]> toTrain = trainingList.subList(0, trainingList.size() / 2);
    for (byte[] b : toTrain) {
      trainer.addSample(b);
    }
    long start = System.currentTimeMillis();
    ByteBuffer dictData = trainer.trainSamplesDirect();
    long end = System.currentTimeMillis();

    LOG.info("Training time {} sample size={}", end - start, totalSize);
    ZstdDictCompress dictCompress = new ZstdDictCompress(dictData, COMP_LEVEL);

    ZstdCompressCtx compContext = new ZstdCompressCtx();
    compContext.loadDict(dictCompress);
    compContext.setLevel(COMP_LEVEL);

    int n = 1;
    LOG.info("Group of:{}", n);
    List<byte[]> group = groupOf(trainingList, n);
    group = group.subList(group.size() / 2, group.size());
    List<byte[]> compresed = compress(compContext, group);

    List<Integer> sizes = group.stream().map(x -> x.length).collect(Collectors.toList());
    ZstdDictDecompress dictDecompress = new ZstdDictDecompress(dictData);
    ZstdDecompressCtx decompContext = new ZstdDecompressCtx();
    decompContext.loadDict(dictDecompress);
    @SuppressWarnings("unused")
    List<byte[]> decompressed = decompress(decompContext, compresed, sizes);

    byte[] buf = new byte[1 << 16];
    decompress(decompContext, compresed, sizes, buf);
    compContext.close();
    decompContext.close();
  }

  private static List<byte[]> compress(ZstdCompressCtx context, List<byte[]> source) {

    ArrayList<byte[]> result = new ArrayList<byte[]>(source.size());
    long start = System.nanoTime();
    long totalSize = 0;
    long compSize = 0;
    for (byte[] b : source) {
      byte[] cb = context.compress(b);
      result.add(cb);
      totalSize += b.length;
      compSize += cb.length;
    }
    long end = System.nanoTime();
    LOG.info("Compression time {} micros total samples={}", (end - start) / 1000, source.size());
    LOG.info("Total size={} compressed={} ratio={}", totalSize, compSize,
      (double) totalSize / compSize);
    LOG.info("Compression speed={}MB/s",
      ((double) totalSize * 1000000000) / ((1 << 20) * (end - start)));
    return result;
  }

  private static List<byte[]> decompress(ZstdDecompressCtx context, List<byte[]> compressed,
      List<Integer> sizes) {
    ArrayList<byte[]> result = new ArrayList<byte[]>(compressed.size());
    int i = 0;
    long decompSize = 0;
    long start = System.nanoTime();

    for (byte[] b : compressed) {
      byte[] db = context.decompress(b, sizes.get(i++));
      decompSize += db.length;
      result.add(db);
    }
    long end = System.nanoTime();
    LOG.info("Decompression time={} size={} speed={} MB/s", (end - start) / 1000, decompSize,
      ((double) decompSize * 1000000000) / ((1 << 20) * (end - start)));

    return result;
  }

  private static void decompress(ZstdDecompressCtx context, List<byte[]> compressed,
      List<Integer> sizes, byte[] buf) {
    int i = 0;
    long decompSize = 0;
    long start = System.nanoTime();

    for (byte[] b : compressed) {

      int size = context.decompressByteArray(buf, 0, sizes.get(i++), b, 0, b.length);
      decompSize += size;
    }
    long end = System.nanoTime();
    LOG.info("Decompression ByteArraytime={} size={} speed={} MB/s", (end - start) / 1000,
      decompSize, ((double) decompSize * 1000000000) / ((1 << 20) * (end - start)));

  }

  private static byte[] next(List<byte[]> list, int start, int count) {

    if (start == list.size()) {
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

  private static List<byte[]> groupOf(List<byte[]> source, int count) {
    ArrayList<byte[]> result = new ArrayList<byte[]>(source.size() / count * count + 1);
    int index = 0;
    while (true) {
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
