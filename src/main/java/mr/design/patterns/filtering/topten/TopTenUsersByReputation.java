package mr.design.patterns.filtering.topten;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import static mr.design.patterns.utils.MRDPUtils.isInteger;
import static mr.design.patterns.utils.MRDPUtils.isNullOrEmpty;
import static mr.design.patterns.utils.MRDPUtils.transformXmlToMap;

public class TopTenUsersByReputation extends Configured implements Tool {

  public static class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {

    // Stores a map of user reputation to the record
    private final TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      Map<String, String> parsed = transformXmlToMap(value.toString());

      String userId = parsed.get("Id");
      String reputation = parsed.get("Reputation");

      if (isNullOrEmpty(userId) || isNullOrEmpty(reputation) || !isInteger(reputation)) {
        return;
      }

      // Add this record to our map with the reputation as the key
      repToRecordMap.put(Integer.parseInt(reputation), new Text(value));

      // If we have more than ten records, remove the one with the lowest rep
      // As this tree map is sorted in descending order, the user with
      // the lowest reputation is the last key
      if (repToRecordMap.size() > 10) {
        repToRecordMap.remove(repToRecordMap.firstKey());
      }
      System.out.println(repToRecordMap.size());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      // Output our ten records to the reducers with a null key
      for (Text t : repToRecordMap.values()) {
        context.write(NullWritable.get(), t);
      }
    }
  }

  public static class TopTenReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

    // Stores a map of user reputation to the record
    // Overloads the comparator to order the reputations in descending order
    private final TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

    @Override
    public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      for (Text value : values) {
        Map<String, String> parsed = transformXmlToMap(value.toString());
        repToRecordMap.put(Integer.parseInt(parsed.get("Reputation")), new Text(value));

        // If we have more than ten records, remove the one with the lowest rep
        // As this tree map is sorted in descending order, the user with
        // the lowest reputation is the last key
        if (repToRecordMap.size() > 10) {
          repToRecordMap.remove(repToRecordMap.firstKey());
        }
      }

      for (Text t : repToRecordMap.descendingMap().values()) {
        // Output our ten records to the file system with a null key
        context.write(NullWritable.get(), t);
      }
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: TopTenUsersByReputation <in> <out>");
      ToolRunner.printGenericCommandUsage(System.err);
      System.exit(2);
    }

    Job job = new Job(conf, "Top Ten Users by Reputation");
    job.setJarByClass(TopTenUsersByReputation.class);
    job.setMapperClass(TopTenMapper.class);
    job.setReducerClass(TopTenReducer.class);
    job.setNumReduceTasks(1);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new TopTenUsersByReputation(), args);
    System.exit(res);
  }
}