package stubs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer's input are local top N from all mappers. We have a single reducer,
 * which creates the final top N.
 * 
 * @author Mahmoud Parsian
 *
 */
public class CountHighLowReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable value = new IntWritable();

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		value.set(sum);
		context.write(key, value);
	}
}
