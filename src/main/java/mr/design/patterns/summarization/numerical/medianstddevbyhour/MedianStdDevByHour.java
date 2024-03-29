package mr.design.patterns.summarization.numerical.medianstddevbyhour;

import mr.design.patterns.summarization.numerical.medianstddev.MedianStdDev;
import mr.design.patterns.summarization.numerical.medianstddev.MedianStdDevTuple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.jline.utils.Log;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import static mr.design.patterns.utils.MRDPUtils.DATE_FORMAT;
import static mr.design.patterns.utils.MRDPUtils.transformXmlToMap;

public class MedianStdDevByHour extends Configured implements Tool {
	public static class MedianStdDevMapper extends Mapper<Object, Text, IntWritable, SortedMapWritable> {
		private IntWritable commentLength = new IntWritable();
		private static final LongWritable ONE = new LongWritable(1);
		private IntWritable outHour = new IntWritable();

		private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Map<String, String> parsed = transformXmlToMap(value.toString());
			// Grab the "CreationDate" field,
			// since it is what we are grouping by
			String strDate = parsed.get("CreationDate");
			// Grab the comment to find the length
			String text = parsed.get("Text");

			// Get the hour this comment was posted in
			Date creationDate = null;
			try {
				creationDate = frmt.parse(strDate);
			} catch (ParseException|NullPointerException e) {
				Log.error("strDate is null");
			}

			if(creationDate == null || text == null){
				return;
			}

			outHour.set(creationDate.getHours());
			commentLength.set(text.length());

			SortedMapWritable outCommentLength = new SortedMapWritable();
			outCommentLength.put(commentLength, ONE);

			// Write out the user ID with min max dates and count
			context.write(outHour, outCommentLength);
		}
	}

	public static class MedianStdDevCombiner extends
			Reducer<IntWritable, SortedMapWritable, IntWritable, SortedMapWritable> {
		protected void reduce(IntWritable key, Iterable<SortedMapWritable> values, Context context)
				throws IOException, InterruptedException {

			SortedMapWritable outValue = new SortedMapWritable();

			for (SortedMapWritable v : values) {
				for (Object entry : v.entrySet()) {
					Entry<WritableComparable, Writable> mapEntry = (Entry<WritableComparable, Writable>) entry;
					LongWritable count = (LongWritable) outValue.get(mapEntry.getKey());

					if (count != null) {
						count.set(count.get() + ((LongWritable) mapEntry.getValue()).get());
					} else {
						outValue.put(mapEntry.getKey(), new LongWritable(((LongWritable) mapEntry.getValue()).get()));
					}
				}
			}
			context.write(key, outValue);
		}
	}

	public static class MedianStdDevReducer extends
			Reducer<IntWritable, SortedMapWritable, IntWritable, MedianStdDevTuple> {
		private MedianStdDevTuple result = new MedianStdDevTuple();
		private TreeMap<Integer, Long> commentLengthCounts = new TreeMap<Integer, Long>();

		public void reduce(IntWritable key, Iterable<SortedMapWritable> values, Context context)
				throws IOException, InterruptedException {
			float sum = 0;
			long totalComments = 0;
			commentLengthCounts.clear();
			result.setMedian(0);
			result.setStdDev(0);

			for (SortedMapWritable v : values) {
				for (Object entry : v.entrySet()) {
					Entry<WritableComparable, Writable> mapEntry = (Entry<WritableComparable, Writable>) entry;
					int length = ((IntWritable) mapEntry.getKey()).get();
					long count = ((LongWritable) mapEntry.getValue()).get();
					totalComments += count;
					sum += length * count;
          commentLengthCounts.merge(length, count, Long::sum);
				}
			}

			long medianIndex = totalComments / 2L;
			long previousComments = 0;
			long comments = 0;
			int prevKey = 0;
			for (Entry<Integer, Long> entry : commentLengthCounts.entrySet()) {
				comments = previousComments + entry.getValue();

				if (previousComments <= medianIndex && medianIndex < comments) {
					if (totalComments % 2 == 0 && previousComments == medianIndex) {
						result.setMedian((float) (entry.getKey() + prevKey) / 2.0f);
					} else {
						result.setMedian(entry.getKey());
					}
					break;
				}

				previousComments = comments;
				prevKey = entry.getKey();
			}

			// calculate standard deviation
			float mean = sum / totalComments;

			float sumOfSquares = 0.0f;
			for (Entry<Integer, Long> entry : commentLengthCounts.entrySet()) {
				sumOfSquares += (entry.getKey() - mean) * (entry.getKey() - mean) * entry.getValue();
			}
			result.setStdDev((float) Math.sqrt(sumOfSquares / (totalComments - 1)));
			context.write(key, result);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: MedianStdDevByHour <in> <out>");
			ToolRunner.printGenericCommandUsage(System.err);
			System.exit(2);
		}

		Job job = new Job(conf, "StackOverflow Median and Standard Deviation Comment Length By Hour");
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(MedianStdDevMapper.class);
		job.setCombinerClass(MedianStdDevCombiner.class);
		job.setReducerClass(MedianStdDevReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(SortedMapWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(MedianStdDevTuple.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MedianStdDevByHour(), args);
		System.exit(res);
	}
}