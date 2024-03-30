package mr.design.patterns.filtering.bloomfilter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
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
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.StringTokenizer;

import mr.design.patterns.filtering.distributedgrep.DistributedGrep;

import static mr.design.patterns.utils.MRDPUtils.transformXmlToMap;

public class BloomFiltering extends Configured implements Tool {
	public static class BloomFilterMapper extends Mapper<Object, Text, Text, NullWritable> {
		private BloomFilter filter = new BloomFilter();

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			// Get file from the DistributedCache
			URI[] files = DistributedCache.getCacheFiles(context.getConfiguration());
			System.out.println("Reading Bloom filter from: " + files[0].getPath());

			// Open local file for read
			DataInputStream strm = new DataInputStream(new FileInputStream(files[0].toString()));

			// Read into our Bloom filter.
			filter.readFields(strm);
			strm.close();
		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Map<String, String> parsed = transformXmlToMap(value.toString());

			// Get the value for the comment
			String comment = parsed.get("Text");
			if (comment == null) return;

			StringTokenizer tokenizer = new StringTokenizer(comment);

			// For each word in the comment
			while (tokenizer.hasMoreTokens()) {
				// If the word is in the filter, output the record and break
				String word = tokenizer.nextToken();
				if (filter.membershipTest(new Key(word.getBytes()))) {
					context.write(value, NullWritable.get());
					break;
				}
			}

		}
	}
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		GenericOptionsParser parser = new GenericOptionsParser(conf, args);
		String[] otherArgs = parser.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: BloomFiltering <in> <out> <bloom_filter_file>");
			ToolRunner.printGenericCommandUsage(System.err);
			System.exit(2);
		}

		DistributedCache.addCacheFile(new URI(otherArgs[2]), conf);
		Job job = new Job(conf, "Bloom Filter");
		job.setJarByClass(BloomFiltering.class);
		job.setMapperClass(BloomFilterMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new BloomFiltering(), args);
		System.exit(res);
	}
}
