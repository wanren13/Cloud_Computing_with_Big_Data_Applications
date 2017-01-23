package stubs;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Example input line: 96.7.4.14 - - [24/Apr/2011:04:20:11 -0400]
 * "GET /cat.jpg HTTP/1.1" 200 12433
 *
 */
public class LogFileMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private Text wordObj = new Text();
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		Pattern pattern = Pattern.compile("(\\d{1,3}(?:\\.\\d{1,3}){3})");
		Matcher matcher = pattern.matcher(line);
		if (matcher.find()) {
			wordObj.set(matcher.group(0));				
			context.write(wordObj, one);
		}
	}
}