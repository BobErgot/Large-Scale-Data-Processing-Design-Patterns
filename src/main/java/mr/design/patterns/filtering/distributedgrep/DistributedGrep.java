package mr.design.patterns.filtering.distributedgrep;

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

import java.io.IOException;

import static mr.design.patterns.filtering.distributedgrep.DistributedGrep.GrepMapper.REGEX_KEY;

public class DistributedGrep extends Configured implements Tool {
  public static class GrepMapper extends Mapper<Object, Text, NullWritable, Text> {

    public static final String REGEX_KEY = "mapregex";
    private String mapRegex = null;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      mapRegex = context.getConfiguration().get(REGEX_KEY, null);
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      if (value.toString().trim().matches(mapRegex)) {
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
      System.err.println("Usage: DistributedGrep <in> <out> <regex> ");
      ToolRunner.printGenericCommandUsage(System.err);
      System.exit(2);
    }
    Job job = new Job(conf, "Distributed Grep");
    job.setJarByClass(DistributedGrep.class);
    job.setMapperClass(GrepMapper.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    job.getConfiguration().set(REGEX_KEY, otherArgs[2]);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new DistributedGrep(), args);
    System.exit(res);
  }
}
