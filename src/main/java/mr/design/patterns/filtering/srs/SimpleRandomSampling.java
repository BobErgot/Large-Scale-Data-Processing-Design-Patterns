package mr.design.patterns.filtering.srs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.jline.utils.Log;

import java.io.IOException;
import java.util.Random;

public class SimpleRandomSampling extends Configured implements Tool {
  public static final String FILTER_PERCENTAGE_KEY = "filter_percentage";

  public static class SimpleRandomSamplingMapper extends Mapper<Object, Text, NullWritable, Text> {
    private Random rands = new Random();
    private Double percentage;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      // Retrieve the percentage that is passed in via the configuration like this: conf.set
      // ("filter_percentage", .5); for .5%
      percentage = context.getConfiguration().getDouble(FILTER_PERCENTAGE_KEY, 0.0);
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      if (rands.nextDouble() < percentage) {
        System.out.println(rands.nextDouble());
        System.out.println(percentage);
        context.write(NullWritable.get(), value);
      }
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    GenericOptionsParser parser = new GenericOptionsParser(conf, args);
    String[] otherArgs = parser.getRemainingArgs();
    if (otherArgs.length != 3) {
      System.err.println("Usage: SimpleRandomSampling <in> <out> <percentage> ");
      ToolRunner.printGenericCommandUsage(System.err);
      System.exit(2);
    }

    double filterPercentage = 0.0;
    try {
      filterPercentage = Double.parseDouble(otherArgs[2]);
    } catch (NumberFormatException e) {
      Log.error("Entered percentage is invalid");
      System.exit(2);
    }

    Job job = new Job(conf, "Simple Random Sampling");
    job.setJarByClass(SimpleRandomSampling.class);
    job.setMapperClass(SimpleRandomSamplingMapper.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(1);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    job.getConfiguration().setDouble(FILTER_PERCENTAGE_KEY, filterPercentage);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new SimpleRandomSampling(), args);
    System.exit(res);
  }
}