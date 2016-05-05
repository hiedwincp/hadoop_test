
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

public class WordCountNew {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);

			while (tokenizer.hasMoreTokens()) {
				value.set(tokenizer.nextToken());
				context.write(value, new IntWritable(1));
			}

		}

	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			// TODO Auto-generated method stub
			for (IntWritable x : values) {
				sum += x.get();
			}
			context.write(key, new IntWritable(sum));

		}

	}
	public static class WordCountPartitioner extends Partitioner<Text, IntWritable> {
		@Override
		public int getPartition(Text key, IntWritable value, int numReduceTasks) {
			if (numReduceTasks == 0)
				return 0;
/*			if(key.equals(new Text("Hadoop")) || key.equals(new Text("Hadoop!"))  ||  key.equals(new Text("Hadoop.")) )
	            return 0;
	        if(key.equals(new Text("Hive")) || key.equals(new Text("Hive,")))
	            return 1;*/
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
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		// JobConf conf = new JobConf(WordCount.class);
		Configuration conf = new Configuration();

		// conf.setJobName("mywc");
		Job job = new Job(conf, "mywc");

		job.setJarByClass(WordCountNew.class);
		
		// set partitioner statement
		job.setPartitionerClass(WordCountPartitioner.class);
		job.setNumReduceTasks(3);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// conf.setMapperClass(Map.class);
		// conf.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		Path outputPath = new Path(args[1]);

		// Configuring the input/output path from the filesystem into the job

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// deleting the output path automatically from hdfs so that we don't
		// have delete it explicitly

//		outputPath.getFileSystem(conf).delete(outputPath);

		// exiting the job only if the flag value becomes false

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
