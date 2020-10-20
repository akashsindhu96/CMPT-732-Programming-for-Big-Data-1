// adapted from https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
//Name: Akash Sindhu
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WikipediaPopular extends Configured implements Tool {

	public static class TokenizerMapper
	extends Mapper<LongWritable, Text, Text, LongWritable>{

		/*
		1. make new object/instance to store values in 
		2. split the lines into words on space 	\\s+"
		3. store first, second and 3rd and 4th word into variable 
		4. if condition for 'en'= yes, 'title== "Main_Page"' = NO, 'title.startsWith("Special:")' = No
		5. store the context.write(date, 4th word)
		*/

		private final static LongWritable one = new LongWritable(1L);
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			StringTokenizer splitted = new StringTokenizer(value.toString());

			// String[] splitted = itr.split("\\s+");

			// String[] date = splitted.get(0);
			// String[] language = splitted.get(1);
			// String[] title = splitted.get(2);
			// String[] visitor = splitted.get(3);

			String date = splitted.nextToken();
			String language = splitted.nextToken();
			String title = splitted.nextToken();
			String visitor = splitted.nextToken();

			if((language.equals("en")) && (!title.equals("Main_Page")) && (!title.startsWith("Special:"))) {

				word.set(date);
				one.set(Long.valueOf(visitor));
				context.write(word, one);
			}
		}
	}

	public static class Maximum_reducer
	extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable result = new LongWritable();

		@Override
		public void reduce(Text date, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

			long max = 0L;

			for (LongWritable val : values) {
				long long_val = val.get();
				if(max < long_val) {
					max = long_val;
				}
			}

			result.set(max);
			context.write(date, result);
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WikipediaPopular(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "Wiki popular");
		job.setJarByClass(WikipediaPopular.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(TokenizerMapper.class);
		// job.setCombinerClass(Maximum_reducer.class);
		job.setReducerClass(Maximum_reducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
