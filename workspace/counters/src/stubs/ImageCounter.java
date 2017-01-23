package stubs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

@SuppressWarnings("unused")
public class ImageCounter extends Configured implements Tool {

	enum ImageType {
		gif,
		jpeg,
		other
	}
	
  @Override
  public int run(String[] args) throws Exception {

    if (args.length != 2) {
      System.out.printf("Usage: ImageCounter <input dir> <output dir>\n");
      return -1;
    }

    Job job = new Job(getConf());
    job.setJarByClass(ImageCounter.class);
    job.setJobName("Image Counter");

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setMapperClass(ImageCounterMapper.class);
    job.setNumReduceTasks(0);

//    job.setOutputKeyClass(Text.class);
//    job.setOutputValueClass(IntWritable.class);
    
    boolean success = job.waitForCompletion(true);
    if (success) {
      /*
       * Print out the counters that the mappers have been incrementing.
       * TODO implement
       */
    	
      System.out.println("The number of .gif is: " + job.getCounters().findCounter(ImageType.gif).getValue());
      System.out.println("The number of .jpeg is: " + job.getCounters().findCounter(ImageType.jpeg).getValue());
      System.out.println("The number of other is: " + job.getCounters().findCounter(ImageType.other).getValue());
      return 0;
    } else
      return 1;

  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Configuration(), new ImageCounter(), args);
    System.exit(exitCode);
  }
}