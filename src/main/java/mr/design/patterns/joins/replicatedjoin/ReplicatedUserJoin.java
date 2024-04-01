package mr.design.patterns.joins.replicatedjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import mr.design.patterns.utils.MRDPUtils;

public class ReplicatedUserJoin extends Configured implements Tool {
  public static final String JOIN_TYPE = "join.type";

  public static class ReplicatedJoinMapper extends Mapper<Object, Text, Text, Text> {
    private static final Text EMPTY_TEXT = new Text("");
    private final HashMap<String, String> userIdToInfo = new HashMap<String, String>();
    private final Text outValue = new Text();
    private String joinType = null;

    public void setup(Context context) throws IOException, InterruptedException {
      // Get file from the DistributedCache
      URI[] files = DistributedCache.getCacheFiles(context.getConfiguration());
      System.out.println("Reading DistributedCache from: " + files[0].getPath());

      // Read all files in the DistributedCache
      for (URI p : files) {
        BufferedReader rdr = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(new File(p.toString())))));
        String line = null;
        // For each record in the user file
        while ((line = rdr.readLine()) != null) {

          // Get the user ID for this record
          Map<String, String> parsed = MRDPUtils.transformXmlToMap(line);
          String userId = parsed.get("Id");

          // Map the user ID to the record
          userIdToInfo.put(userId, line);
        }
        rdr.close();
      }
      // Get the join type from the configuration
      joinType = context.getConfiguration().get(JOIN_TYPE);
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());

      String userId = parsed.get("UserId");
      String userInformation = userIdToInfo.get(userId);

      // If the user information is not null, then output
      if (userInformation != null) {
        outValue.set(userInformation);
        context.write(value, outValue);
      } else if (joinType.equalsIgnoreCase("leftouter")) {
        // If we are doing a left outer join, output the record with an empty value
        context.write(value, EMPTY_TEXT);
      }
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    GenericOptionsParser parser = new GenericOptionsParser(conf, args);
    String[] otherArgs = parser.getRemainingArgs();
    if (otherArgs.length != 4) {
      System.err.println("Usage: ReplicatedUserJoin  <comments_in> <out> <user_in> <join_type>");
      ToolRunner.printGenericCommandUsage(System.err);
      System.exit(2);
    }
    DistributedCache.addCacheFile(new URI(otherArgs[2]), conf);
    Job job = new Job(conf, "ReplicatedUserJoin");
    job.setJarByClass(ReplicatedUserJoin.class);
    job.setMapperClass(ReplicatedJoinMapper.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    job.getConfiguration().set(JOIN_TYPE, otherArgs[3]);

    job.setReducerClass(Reducer.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    return job.waitForCompletion(true) ? 0 : 2;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new ReplicatedUserJoin(), args);
    System.exit(res);
  }
}
