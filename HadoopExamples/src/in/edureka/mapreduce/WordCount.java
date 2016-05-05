package in.edureka.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

public class WordCount {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		// JobConf conf = new JobConf(WordCount.class);
		Configuration conf = new Configuration();

		// conf.setJobName("mywc");
		Job job = new Job(conf, "mywc");

		job.setJarByClass(WordCount.class);
		job.setMapperClass(WCMap.class);
		job.setReducerClass(WCReduce.class);

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

		outputPath.getFileSystem(conf).delete(outputPath);

		// exiting the job only if the flag value becomes false

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
