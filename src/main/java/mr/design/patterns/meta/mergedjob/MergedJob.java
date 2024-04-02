package mr.design.patterns.meta.mergedjob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import mr.design.patterns.filtering.distinct.DistinctUser;
import mr.design.patterns.utils.MRDPUtils;

public class MergedJob extends Configured implements Tool {

  public static final String MULTIPLE_OUTPUTS_ANONYMIZE = "anonymize";
  public static final String MULTIPLE_OUTPUTS_DISTINCT = "distinct";

  public static class AnonymizeDistinctMergedMapper extends Mapper<Object, Text, TaggedText, Text> {

    private static final Text DISTINCT_OUT_VALUE = new Text();

    private final Random rndm = new Random();
    private final TaggedText anonymizeOutkey = new TaggedText();
    private final TaggedText distinctOutkey = new TaggedText();
    private final Text anonymizeOutvalue = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      anonymizeMap(key, value, context);
      distinctMap(key, value, context);
    }

    private void anonymizeMap(Object key, Text value, Context context) throws IOException, InterruptedException {
      // Parse the input string into a nice map
      Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());

      if (parsed.size() > 0) {
        StringBuilder bldr = new StringBuilder();
        bldr.append("<row ");
        for (Entry<String, String> entry : parsed.entrySet()) {

          if (entry.getKey().equals("UserId") || entry.getKey().equals("Id")) {
            // ignore these fields
          } else if (entry.getKey().equals("CreationDate")) {
            // Strip out the time, anything after the 'T' in the value
            bldr.append(entry.getKey() + "=\"" + entry.getValue().substring(0, entry.getValue().indexOf('T')) + "\" ");
          } else {
            // Otherwise, output this.
            bldr.append(entry.getKey() + "=\"" + entry.getValue() + "\" ");
          }

        }
        bldr.append(">");
        anonymizeOutkey.setTag("A");
        anonymizeOutkey.setText(Integer.toString(rndm.nextInt()));
        anonymizeOutvalue.set(bldr.toString());
        context.write(anonymizeOutkey, anonymizeOutvalue);
      }
    }

    private void distinctMap(Object key, Text value, Context context) throws IOException, InterruptedException {
      // Parse the input into a nice map.
      Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());

      // Get the value for the UserId attribute
      String userId = parsed.get("UserId");

      // If it is null, skip this record
      if (userId == null) {
        return;
      }

      // Otherwise, set our output key to the user's id, tagged with a "D"
      distinctOutkey.setTag("D");
      distinctOutkey.setText(userId);

      // Write the user's id with a null value
      context.write(distinctOutkey, DISTINCT_OUT_VALUE);
    }
  }

  public static class AnonymizeDistinctMergedReducer extends Reducer<TaggedText, Text, Text, NullWritable> {

    private MultipleOutputs<Text, NullWritable> mos = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      mos = new MultipleOutputs<Text, NullWritable>(context);
    }

    @Override
    protected void reduce(TaggedText key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

      if (key.getTag().equals("A")) {
        anonymizeReduce(key.getText(), values, context);
      } else {
        distinctReduce(key.getText(), values, context);
      }
    }

    private void anonymizeReduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

      for (Text value : values) {
        mos.write(MULTIPLE_OUTPUTS_ANONYMIZE, value, NullWritable.get(), MULTIPLE_OUTPUTS_ANONYMIZE + "/part");
      }
    }

    private void distinctReduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      mos.write(MULTIPLE_OUTPUTS_DISTINCT, key, NullWritable.get(), MULTIPLE_OUTPUTS_DISTINCT + "/part");
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      mos.close();
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: MergedJob <comment data> <out>");
      System.exit(1);
    }

    // Configure the merged job
    Job job = new Job(conf, "MergedJob");
    job.setJarByClass(MergedJob.class);

    job.setMapperClass(AnonymizeDistinctMergedMapper.class);
    job.setReducerClass(AnonymizeDistinctMergedReducer.class);
    job.setNumReduceTasks(10);

    TextInputFormat.setInputPaths(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    MultipleOutputs.addNamedOutput(job, MULTIPLE_OUTPUTS_ANONYMIZE, TextOutputFormat.class, Text.class, NullWritable.class);
    MultipleOutputs.addNamedOutput(job, MULTIPLE_OUTPUTS_DISTINCT, TextOutputFormat.class, Text.class, NullWritable.class);

    job.setOutputKeyClass(TaggedText.class);
    job.setOutputValueClass(Text.class);

    return job.waitForCompletion(true) ? 0 : 2;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new MergedJob(), args);
    System.exit(res);
  }
}
