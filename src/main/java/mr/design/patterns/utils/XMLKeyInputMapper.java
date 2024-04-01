package mr.design.patterns.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Map;

public class XMLKeyInputMapper extends Configured implements Tool {

	public static class XmlMapper extends Mapper<Object, Text, Text, Text> {
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		private String keyToRetrieve;

		@Override
		protected void setup(Context context) {
			Configuration conf = context.getConfiguration();
			keyToRetrieve = conf.get("keyToRetrieve");
		}

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());
			String keyValue = parsed.get(keyToRetrieve);
			if (keyValue != null && !keyValue.isEmpty()) {
				outputKey.set(keyValue);
				outputValue.set(value);

				context.write(outputKey, outputValue);
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: XMLKeyInputMapper <in> <out> <keyToRetrieve>");
			System.exit(2);
		}

		conf.set("keyToRetrieve", otherArgs[2]);

		Job job = Job.getInstance(conf, "XML Key Input Mapper");
		job.setJarByClass(XMLKeyInputMapper.class);
		job.setMapperClass(XmlMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(0);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new XMLKeyInputMapper(), args);
		System.exit(res);
	}
}
