package stubs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

@SuppressWarnings("unused")
public class LetterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	boolean caseSensitive = true;
	private static final Logger LOGGER = Logger.getLogger(LetterMapper.class.getName());
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();

		for (String word : line.split("\\W+")) {
			if (word.length() > 0) {
				String letter = word.substring(0, 1);
				if(!caseSensitive)
					letter = letter.toLowerCase();				
				
				context.write(new Text(letter), new IntWritable(word.length()));
			}
		}
	}
	
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		caseSensitive = conf.getBoolean("caseSensitive", true);
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("caseSensitive is " + (caseSensitive ? "true" : "false"));
		}
	}
}
