package stubs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper's input records are:
 *
 * <key-as-string><,><value-as-integer>
 *
 * This class, AggregateByKeyMapper, generates all (K,V) pairs. Note that K's
 * can have duplicates. For example it might generate: (K,2) and (K,3).
 *
 *
 * @author Mahmoud Parsian
 *
 */
public class AvgRatingPerUserMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

	// reuse objects
	private IntWritable K2 = new IntWritable();
	private DoubleWritable V2 = new DoubleWritable();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String valueAsString = value.toString().trim();
		String[] tokens = valueAsString.split(",");
		if (tokens.length != 3) {
			return;
		}

		int user_id = Integer.parseInt(tokens[1]);
		double rating = Double.parseDouble(tokens[2]);
		K2.set(user_id);
		V2.set(rating);
		context.write(K2, V2);
	}
}
