package in.edureka.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public  class WCReduce extends
		Reducer<Text, IntWritable, Text, IntWritable> {

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
