package mr.design.patterns.summarization.invertedindex;

import mr.design.patterns.utils.MRDPUtils;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.io.IOException;
import java.util.Map;

public class WikipediaExtractor extends Configured implements Tool {

	public static class WikipediaUrlMapper extends Mapper<Object, Text, Text, Text> {

		private Text link = new Text();
		private Text outKey = new Text();

		// Pattern to match Wikipedia URLs
		private static final Pattern WIKIPEDIA_URL_PATTERN = Pattern.compile("(https?://[a-z]+\\.wikipedia\\.org/wiki/[^#\\s\"<>]+)");

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());

			// Grab the necessary XML attributes
			String txt = parsed.get("Body");
			String postType = parsed.get("PostTypeId");
			String rowId = parsed.get("Id");

			// if the body is null, or the post is a question (1), skip
			if (txt == null || (postType != null && postType.equals("1"))) {
				return;
			}

			// Unescape the HTML because the SO data is escaped.
			txt = StringEscapeUtils.unescapeHtml(txt.toLowerCase());

			outKey.set(rowId);
			getWikipediaURL(txt, context);
		}

		private void getWikipediaURL(String body, Context context) throws IOException, InterruptedException {
			Matcher matcher = WIKIPEDIA_URL_PATTERN.matcher(body);

			while (matcher.find()) {
				String foundUrl = matcher.group();
				link.set(foundUrl);
				context.write(link, outKey);
			}
		}
	}
	
	public static class Concatenator extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			StringBuilder sb = new StringBuilder();
			boolean first = true;
			for (Text id : values) {
				if (first) {
					first = false;
				} else {
					sb.append(" ");
				}
				sb.append(id.toString());
			}

			result.set(sb.toString());
			context.write(key, result);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: WikipediaExtractor <in> <out>");
			ToolRunner.printGenericCommandUsage(System.err);
			System.exit(2);
		}

		Job job = new Job(conf, "StackOverflow Answer to Wikipedia URL Reverse Index Creation");
		job.setJarByClass(WikipediaExtractor.class);
		job.setMapperClass(WikipediaUrlMapper.class);
		job.setCombinerClass(Concatenator.class);
		job.setReducerClass(Concatenator.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WikipediaExtractor(), args);
		System.exit(res);
	}
}
