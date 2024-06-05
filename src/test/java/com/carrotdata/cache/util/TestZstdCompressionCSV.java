package com.carrotdata.cache.util;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDecompressCtx;
import com.github.luben.zstd.ZstdDictCompress;
import com.github.luben.zstd.ZstdDictDecompress;
import com.github.luben.zstd.ZstdDictTrainer;

public class TestZstdCompressionCSV {
  private static final Logger LOG = LoggerFactory.getLogger(TestZstdCompressionCSV.class);

  private static int DICT_SIZE = 1 << 20; // 16KB
  private static int COMP_LEVEL = 3;

  @SuppressWarnings("unused")
  public static void main(String[] args) throws IOException {
    // String file = "/Users/vrodionov/Development/datasets/spotify/spotify.csv";
    // String file =
    // "/Users/vrodionov/Development/datasets/twitter_sentiments/training.1600000.processed.noemoticon.csv";
    // String file = "/Users/vrodionov/Development/datasets/amazon_product_review/Reviews.csv";
    // String file = "/Users/vrodionov/Development/datasets/airbnb/Airbnb_Data.csv";
    // String file = "/Users/vrodionov/Development/datasets/arxiv/arxiv-metadata-oai-snapshot.json";
    // String file = "/Users/vrodionov/Development/datasets/dblp/dblp.json";
    // String file = "/Users/vrodionov/Development/datasets/ohio/higher_ed_employee_salaries.csv";
    String file = "/Users/vrodionov/Development/datasets/twitter/twitter.twitter2.json";

    File f = new File(file);
    long fileSize = f.length();
    BufferedInputStream bis = new BufferedInputStream(new FileInputStream(f), 1 << 16);
    DataInputStream dis = new DataInputStream(bis);
    // Load all files
    int totalSize = 0;
    // Stream<String> lines = Files.lines(Path.of(file));
    List<byte[]> toTrain = new ArrayList<byte[]>();
    LOG.info("Compile training set");
    totalSize = getTrainingList(dis, toTrain, fileSize);
    dis.close();

    LOG.info("Avg line length={} training set size={}", totalSize / toTrain.size(), totalSize);

    ZstdDictTrainer trainer = new ZstdDictTrainer(totalSize, DICT_SIZE);
    for (byte[] bb : toTrain) {
      trainer.addSample(bb);
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
    LOG.info("Compression starts");
    List<Integer> sizes = new ArrayList<Integer>();
    bis = new BufferedInputStream(new FileInputStream(f), 1 << 20);
    dis = new DataInputStream(bis);
    List<byte[]> compresed = compress(compContext, dis, sizes, n);
    dis.close();

    ZstdDictDecompress dictDecompress = new ZstdDictDecompress(dictData);
    ZstdDecompressCtx decompContext = new ZstdDecompressCtx();
    decompContext.loadDict(dictDecompress);

    byte[] buf = new byte[1 << 20];
    decompress(decompContext, compresed, sizes, buf);

    compContext.close();
    decompContext.close();
  }

  @SuppressWarnings("deprecation")
  private static int getTrainingList(DataInputStream dis, List<byte[]> toTrain, long fileSize)
      throws IOException {
    int reqSize = 100 * DICT_SIZE;
    double ratio = (double) reqSize / fileSize;
    ThreadLocalRandom tlr = ThreadLocalRandom.current();
    int size = 0;
    String line = null;
    while ((line = dis.readLine()) != null) {

      // LOG.info(s);
      double d = tlr.nextDouble();
      if (d <= ratio) {
        byte[] b = line.getBytes();
        toTrain.add(b);
        size += b.length;
      }
    }
    return size;
  }

  @SuppressWarnings("deprecation")
  private static List<byte[]> compress(ZstdCompressCtx context, DataInputStream source,
      List<Integer> sizes, int group) throws IOException {

    ArrayList<byte[]> result = new ArrayList<byte[]>();
    long start = System.nanoTime();
    long totalSize = 0;
    long compSize = 0;
    int gc = 0;
    int totalLines = 0;
    byte[] current = null;
    int prevCounter = 0;
    long prevTime = System.currentTimeMillis();
    String line = null;
    while ((line = source.readLine()) != null) {
      totalLines++;
      byte[] b = line.getBytes();
      if (++gc <= group) {
        current = append(current, b);
        continue;
      }
      byte[] cb = context.compress(current);
      result.add(cb);
      sizes.add(current.length);
      totalSize += current.length;
      int c = (int) (totalSize / 100_000_000);
      if (c > prevCounter) {
        prevCounter = c;
        LOG.info("Compressed {} last batch time={}", c * 100_000_000L,
          System.currentTimeMillis() - prevTime);
        prevTime = System.currentTimeMillis();
      }
      compSize += cb.length;
      current = b;
      gc = 0;
    }
    byte[] cb = context.compress(current);
    result.add(cb);
    sizes.add(current.length);
    totalSize += current.length;
    compSize += cb.length;
    long end = System.nanoTime();
    LOG.info("Compression time {} micros total samples={}", (end - start) / 1000, totalLines);
    LOG.info("Total size={} compressed={} ratio={}", totalSize, compSize,
      (double) totalSize / compSize);
    LOG.info("Compression speed={}MB/s",
      ((double) totalSize * 1000000000) / ((1L << 20) * (end - start)));
    return result;
  }

  private static byte[] append(byte[] b1, byte[] b2) {
    if (b1 == null) return b2;
    byte[] bb = new byte[b1.length + b2.length];
    System.arraycopy(b1, 0, bb, 0, b1.length);
    System.arraycopy(b2, 0, bb, b1.length, b2.length);
    return bb;
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

}
