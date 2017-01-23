package stubs;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int letterCount = 0;
		int lengthSum = 0;

		for (IntWritable value : values) {
			lengthSum += value.get();
			letterCount++;
		}

		context.write(key, new DoubleWritable(lengthSum / (double) letterCount));
	}
}