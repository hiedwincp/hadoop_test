import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountWithPartition extends Configured implements Tool {

	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static class WordCountPartitioner extends Partitioner<Text, IntWritable> {
		@Override
		public int getPartition(Text key, IntWritable value, int numReduceTasks) {
			if (numReduceTasks == 0)
				return 0;
			String keyString = key.toString().toLowerCase();
			if (keyString.startsWith("a") || keyString.startsWith("b") || keyString.startsWith("c")
					|| keyString.startsWith("d") || keyString.startsWith("e"))
				return 0;
			if (keyString.startsWith("f") || keyString.startsWith("g") || keyString.startsWith("h")
					|| keyString.startsWith("i") || keyString.startsWith("j"))
				return 1;
			else
				return 2;
		}

	}

	@Override
	public int run(String[] arg) throws Exception {

		Configuration conf = getConf();

		Job job = new Job(conf, "wordcount");
		job.setJarByClass(WordCountMapper.class);

		FileInputFormat.setInputPaths(job, new Path(arg[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg[1]));


		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);


		job.setMapperClass(WordCountMapper.class);
		job.setCombinerClass(WordCountReducer.class);
		
		// set partitioner statement
//		job.setPartitionerClass(WordCountPartitioner.class);
		job.setReducerClass(WordCountReducer.class);
//		job.setNumReduceTasks(3);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(), new WordCountWithPartition(), args);
		System.exit(0);

	}
}
