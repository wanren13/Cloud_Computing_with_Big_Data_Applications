package stubs;

import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
//import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;

public class TestProcessLogs {

	/*
	 * Declare harnesses that let you test a mapper, a reducer, and a mapper and
	 * a reducer working together.
	 */
	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

	/*
	 * Set up the test. This method will be called before every test.
	 */
	@Before
	public void setUp() {

		/*
		 * Set up the mapper test harness.
		 */
		LogFileMapper mapper = new LogFileMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
		mapDriver.setMapper(mapper);

		/*
		 * Set up the reducer test harness.
		 */
		SumReducer reducer = new SumReducer();
		reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
		reduceDriver.setReducer(reducer);

		/*
		 * Set up the mapper/reducer test harness.
		 */
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}

	// No now is definitely not the best time

	/*
	 * Test the mapper.
	 */
	@Test
	public void testMapper() {
		mapDriver
				.withInput(new LongWritable(1),
						new Text(
								"10.223.157.186 - - [15/Jul/2009:21:24:17 -0700] \"GET /assets/img/media.jpg HTTP/1.1\" 200 110997"))
				.withOutput(new Text("10.223.157.186"), new IntWritable(1)).runTest();

		System.out.println("Mapper passed test");
	}

	/*
	 * Test the reducer.
	 */
	@Test
  public void testReducer() {
	  reduceDriver	  
	  .withInput(new Text("10.223.157.186"), 
			  Arrays.asList(new IntWritable(1), new IntWritable(1)))
	  .withOutput(new Text("10.223.157.186"), new IntWritable(2))	  
	  .runTest();
	  
	  System.out.println("Reducer passed test");
  }

	/*
	 * Test the mapper and reducer working together.
	 */
	@Test
  public void testMapReduce() {
	  
	  mapReduceDriver
	  .withInput(new LongWritable(1), 
			  new Text("10.223.157.186 - - [15/Jul/2009:21:24:17 -0700] \"GET /assets/img/media.jpg HTTP/1.1\" 200 110997\n"))
	  .withInput(new LongWritable(1), 
			  new Text("10.223.157.186 - - [15/Jul/2009:21:24:18 -0700] \"GET /assets/img/pdf-icon.gif HTTP/1.1\" 200 228\n"))
	  .withInput(new LongWritable(1), 
			  new Text("10.216.113.172 - - [16/Jul/2009:02:51:28 -0700] \"GET / HTTP/1.1\" 200 7616\n"))
	  .withInput(new LongWritable(1), 
			  new Text("10.216.113.172 - - [16/Jul/2009:02:51:29 -0700] \"GET /assets/js/lowpro.js HTTP/1 .1\" 200 10469\n"))
	  .withInput(new LongWritable(1), 
			  new Text("10.216.113.172 - - [16/Jul/2009:02:51:29 -0700] \"GET /assets/css/reset.css HTTP/1.1\" 200 1014\n"))
	  .withOutput(new Text("10.216.113.172"), new IntWritable(3))
	  .withOutput(new Text("10.223.157.186"), new IntWritable(2))	  
	  .runTest();

	  System.out.println("MapReduce passed test");
  }
}
