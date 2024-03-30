package mr.design.patterns.filtering.bloomfilter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

import static java.lang.Math.log;
import static java.lang.Math.pow;

public class BloomFilterDriver {
  public static void main(String[] args) throws Exception {
    // Parse command line arguments
    Path inputFile = new Path(args[0]);

    // http://hur.st/bloomfilter
    // number of items in the filter
    int numMembers = Integer.parseInt(args[2]);

    float falsePosRate = Float.parseFloat(args[3]);

    //output file
    Path bfFile = new Path(args[1]);

    // Calculate our vector size and optimal K value based on approximations
    int vectorSize = getOptimalBloomFilterSize(numMembers, falsePosRate);
    int nbHash = getOptimalK(numMembers, vectorSize);

    // Create new Bloom filter
    BloomFilter filter = new BloomFilter(vectorSize, nbHash, Hash.MURMUR_HASH);

    System.out.println("Training Bloom filter of size " + vectorSize + " with " + nbHash + " hash functions, " + numMembers + " approximate number of records, and " + falsePosRate + " false positive rate");

    // Open file for read
    String line = null;
    int numElements = 0;
    FileSystem fs = FileSystem.get(new Configuration());

    for (FileStatus status : fs.listStatus(inputFile)) {
      InputStream stream;
      if (status.getPath().toString().toLowerCase().endsWith("gz")) {
        stream = new GZIPInputStream(fs.open(status.getPath()));
      } else {
        stream = fs.open(status.getPath());
      }

      BufferedReader rdr = new BufferedReader(new InputStreamReader(stream));
      System.out.println("Reading " + status.getPath());
      while ((line = rdr.readLine()) != null) {
        filter.add(new Key(line.getBytes()));
        ++numElements;
      }

      rdr.close();
    }

    System.out.println("Trained Bloom filter with " + numElements + " entries.");
    System.out.println("Serializing Bloom filter to HDFS at " + bfFile);

    FSDataOutputStream strm = fs.create(bfFile);
    filter.write(strm);
    strm.flush();
    strm.close();
    System.exit(0);
  }

  /**
   * Gets the optimal Bloom filter sized based on the input parameters and the optimal number of
   * hash functions.
   *
   * @param numElements  - The number of elements used to train the set.
   * @param falsePosRate - The desired false positive rate.
   * @return The optimal Bloom filter size.
   */
  public static int getOptimalBloomFilterSize(int numElements, float falsePosRate) {
    return (int) (-numElements * Math.log(falsePosRate) / (Math.log(2) * Math.log(2)));
  }

  /**
   * Gets the optimal-k value based on the input parameters.
   *
   * @param numElements - The number of elements used to train the set.
   * @param vectorSize  - The size of the Bloom filter.
   * @return The optimal-k value, rounded to the closest integer.
   */
  public static int getOptimalK(float numElements, float vectorSize) {
    return (int) Math.round(vectorSize * Math.log(2) / numElements);
  }
}
