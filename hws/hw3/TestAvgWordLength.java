package stubs;

import java.util.Arrays;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
//import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;

public class TestAvgWordLength {

  /*
   * Declare harnesses that let you test a mapper, a reducer, and
   * a mapper and a reducer working together.
   */
  MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
  ReduceDriver<Text, IntWritable, Text, DoubleWritable> reduceDriver;
  MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, DoubleWritable> mapReduceDriver;

  /*
   * Set up the test. This method will be called before every test.
   */
  @Before
  public void setUp() {

    /*
     * Set up the mapper test harness.
     */
    LetterMapper mapper = new LetterMapper();
    mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
    mapDriver.setMapper(mapper);

    /*
     * Set up the reducer test harness.
     */
    AverageReducer reducer = new AverageReducer();
    reduceDriver = new ReduceDriver<Text, IntWritable, Text, DoubleWritable>();
    reduceDriver.setReducer(reducer);

    /*
     * Set up the mapper/reducer test harness.
     */
    mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, DoubleWritable>();
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
	  .withInput(new LongWritable(1), new Text("No now is definitely not the best time"))
	  .withOutput(new Text("N"), new IntWritable(2))
	  .withOutput(new Text("n"), new IntWritable(3))
	  .withOutput(new Text("i"), new IntWritable(2))
	  .withOutput(new Text("d"), new IntWritable(10))
	  .withOutput(new Text("n"), new IntWritable(3))
	  .withOutput(new Text("t"), new IntWritable(3))
	  .withOutput(new Text("b"), new IntWritable(4))
	  .withOutput(new Text("t"), new IntWritable(4))
	  .runTest();
	    
	  System.out.println("Mapper passed test");
  }

  /*
   * Test the reducer.
   */
  @Test
  public void testReducer() {
	  reduceDriver	  
	  .withInput(new Text("t"), 
			  Arrays.asList(new IntWritable(2), new IntWritable(2), new IntWritable(3), new IntWritable(2)))
	  .withOutput(new Text("t"), new DoubleWritable(2.25))	  
	  .runTest();
	  
	  System.out.println("Reducer passed test");
  }


  /*
   * Test the mapper and reducer working together.
   */
  @Test
  public void testMapReduce() {
	  mapReduceDriver
	  .withInput(new LongWritable(1), new Text("No now is definitely not the best time"))
	  .withOutput(new Text("N"), new DoubleWritable(2))
	  .withOutput(new Text("b"), new DoubleWritable(4))
	  .withOutput(new Text("d"), new DoubleWritable(10))
	  .withOutput(new Text("i"), new DoubleWritable(2))
	  .withOutput(new Text("n"), new DoubleWritable(3))
	  .withOutput(new Text("t"), new DoubleWritable(3.5))
	  .runTest();

	  System.out.println("MapReduce passed test");
  }
}
