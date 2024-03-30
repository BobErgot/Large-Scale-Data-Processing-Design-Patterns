package mr.design.patterns.organization.shuffling;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import java.util.Map.Entry;
import java.util.Random;

import mr.design.patterns.organization.binning.BinningTags;
import mr.design.patterns.utils.MRDPUtils;

public class Anonymize extends Configured implements Tool {

	public static class AnonymizeMapper extends Mapper<Object, Text, IntWritable, Text> {
		private IntWritable outKey = new IntWritable();
		private Random rndm = new Random();
		private Text outValue = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());

			if (!parsed.isEmpty()) {
				StringBuilder bldr = new StringBuilder();
				// Create the start of the record
				bldr.append("<row ");

				// For each XML attribute
				for (Entry<String, String> entry : parsed.entrySet()) {

					// If it is a user ID or row ID, ignore it
					if (entry.getKey().equals("UserId") || entry.getKey().equals("Id")) {
						continue;
					} else if (entry.getKey().equals("CreationDate")) {
						// If it is a CreationDate, remove the time from the date i.e., anything after the
						// 'T' in the value
						bldr.append(entry.getKey() + "=\"" + entry.getValue().substring(0, entry.getValue().indexOf('T')) + "\" ");
					} else {
						// Otherwise, output the attribute and value as is
						bldr.append(entry.getKey() + "=\"" + entry.getValue() + "\" ");
					}
				}
				// Add the /> to finish the record
				bldr.append("/>");

				// Set the sort key to a random value and output
				outKey.set(rndm.nextInt());
				outValue.set(bldr.toString());
				context.write(outKey, outValue);
			}
		}
	}

	public static class ValueReducer extends Reducer<IntWritable, Text, Text, NullWritable> {
		protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text t : values) {
				context.write(t, NullWritable.get());
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		GenericOptionsParser parser = new GenericOptionsParser(conf, args);
		String[] otherArgs = parser.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: Anonymize <in> <out>");
			ToolRunner.printGenericCommandUsage(System.err);
			System.exit(2);
		}
		Job job = new Job(conf, "Anonymize");
		job.setJarByClass(Anonymize.class);
		job.setMapperClass(AnonymizeMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(ValueReducer.class);
		job.setNumReduceTasks(4);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Anonymize(), args);
		System.exit(res);
	}
}
