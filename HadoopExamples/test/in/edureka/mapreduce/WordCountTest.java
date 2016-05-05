package in.edureka.mapreduce;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class WordCountTest {
	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

	@Before
	public void setUp() {
		WCMap mapper = new WCMap();
		// mapDriver = MapDriver.newMapDriver(new WCMap());
		mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
		mapDriver.setMapper(mapper);

		WCReduce reducer = new WCReduce();
		reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
		reduceDriver.setReducer(reducer);

		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}

	@Test
	public void testMapperWithSingle() {
		try {
			mapDriver.withInput(new LongWritable(), new Text(" sunday   "))
					.withOutput(new Text("sunday"), new IntWritable(1))
					.runTest();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testMapperWithMultiple() {
		try {
			mapDriver.withInput(new LongWritable(1), new Text("cat cat dog"));
			mapDriver.withOutput(new Text("cat"), new IntWritable(1));
			mapDriver.withOutput(new Text("cat"), new IntWritable(1));
			mapDriver.withOutput(new Text("dog"), new IntWritable(1));
			mapDriver.runTest();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testReducerWithSingle() {
		try {
			List<IntWritable> values = new ArrayList<IntWritable>();
			values.add(new IntWritable(1));
			values.add(new IntWritable(1));
			reduceDriver.withInput(new Text("cat"), values);
			reduceDriver.withOutput(new Text("cat"), new IntWritable(2));
			reduceDriver.runTest();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testReducerWithMultiple() {
		try {
			List<IntWritable> values = new ArrayList<IntWritable>();
			values.add(new IntWritable(1));
			values.add(new IntWritable(1));
			reduceDriver.withInput(new Text("cat"), values);
			reduceDriver.withOutput(new Text("cat"), new IntWritable(2));

			values.add(new IntWritable(1));
			reduceDriver.withInput(new Text("dog"), values);
			reduceDriver.withOutput(new Text("dog"), new IntWritable(3));

			reduceDriver.runTest();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testMapReduceSimple() {

		try {
			mapReduceDriver.withInput(new LongWritable(1), new Text(
					"cat cat dog"));
			mapReduceDriver.addOutput(new Text("cat"), new IntWritable(2));
			mapReduceDriver.addOutput(new Text("dog"), new IntWritable(1));
			mapReduceDriver.runTest();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testMapReduceComplex() {

		try {
			mapReduceDriver.withInput(new LongWritable(1), new Text(
					"cat cat dog\n hat cat     dog  \n cat  hat    dog\n hat mat"));
			mapReduceDriver.addOutput(new Text("cat"), new IntWritable(4));
			mapReduceDriver.addOutput(new Text("dog"), new IntWritable(3));
			mapReduceDriver.addOutput(new Text("hat"), new IntWritable(3));
			mapReduceDriver.addOutput(new Text("mat"), new IntWritable(1));
			mapReduceDriver.runTest();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
