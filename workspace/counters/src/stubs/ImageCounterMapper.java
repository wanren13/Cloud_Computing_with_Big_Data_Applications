package stubs;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import stubs.ImageCounter.ImageType;

/**
 * Example input line:
 * 96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] "GET /cat.jpg HTTP/1.1" 200 12433
 *
 */
public class ImageCounterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

//	enum ImageType {
//		gif,
//		jpeg,
//		other
//	}
	
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

	  if (value.toString().contains(".gif"))
		  context.getCounter(ImageType.gif).increment(1);
	  else if (value.toString().contains(".jpeg"))
		  context.getCounter(ImageType.jpeg).increment(1);
	  else
		  context.getCounter(ImageType.other).increment(1);

  }
}