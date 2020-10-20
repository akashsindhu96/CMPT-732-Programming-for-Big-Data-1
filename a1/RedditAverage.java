
// adapted from https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.util.regex.Pattern;
import org.json.JSONObject;
import org.apache.hadoop.io.DoubleWritable;


public class RedditAverage extends Configured implements Tool {

	public static class TokenizerMapper
	extends Mapper<LongWritable, Text, Text, LongPairWritable>{

		private LongPairWritable pair = new LongPairWritable();
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {

			JSONObject record = new JSONObject(value.toString());

			// get subreddit name from JSON object
			String subreddit = (String) record.get("subreddit");

			// set it to key 
			word.set(subreddit);

			// get the pair but before we need to have the score and then pass it to longpairwritable class instance 
			int score = (int) record.get("score");

			pair.set(1, Long.valueOf(score));

			//write to context 
			context.write(word, pair);





			// Pattern word_sep = Pattern.compile("[\\p{Punct}\\s]+");
			// String[] sep_words_arr = word_sep.split(value.toString());
			// // StringTokenizer itr = new StringTokenizer(value.toString());

			// for (String each_word:sep_words_arr){
			// 	String lowercase_word = each_word.toLowerCase();
			// 	word.set(lowercase_word);
			// 	context.write(word, one);
			

			// while (itr.hasMoreTokens()) {
			// 	word.set(itr.nextToken());
			// 	context.write(word, one);
			// }
		}
	}

	public static class LongPairCombiner
	extends Reducer<Text, LongPairWritable, Text, LongPairWritable> {

		private LongPairWritable result = new LongPairWritable();

		@Override
		public void reduce(Text key, Iterable<LongPairWritable> values,
				Context context
				) throws IOException, InterruptedException {

			long sum = 0;
			long comments = 0;
			for (LongPairWritable val : values) {
				sum += val.get_1();
				comments += val.get_0();
			}

			result.set(comments, sum);
			context.write(key, result);

		}

	}

	public static class AverageReducer
	extends Reducer<Text, LongPairWritable, Text, DoubleWritable> {

		private DoubleWritable result = new DoubleWritable();

		@Override
		public void reduce(Text key, Iterable<LongPairWritable> values,
				Context context
				) throws IOException, InterruptedException {

			long total_sum = 0;
			long comments = 0;
			for (LongPairWritable val : values) {
				total_sum += val.get_1();
				comments += val.get_0();
			}

			double average = total_sum/(double) comments;

			result.set(average);
			context.write(key, result);

		}

	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RedditAverage(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(RedditAverage.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(LongPairCombiner.class);
		job.setReducerClass(AverageReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongPairWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
