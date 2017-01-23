package stubs;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class ReadCompressedSequenceFile extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    if (args.length != 2) {
      System.out
          .printf("Usage: ReadCompressedSequenceFile <input dir> <output dir>\n");
      return -1;
    }

    Job job = Job.getInstance(getConf());
    job.setJarByClass(ReadCompressedSequenceFile.class);
    job.setJobName("Read Compressed Sequence File");

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    /*
     * TODO implement
     */
    
    job.setInputFormatClass(SequenceFileInputFormat.class);
    
    job.setMapperClass(SEQReadMapper.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
    

    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Configuration(), new ReadCompressedSequenceFile(), args);
    System.exit(exitCode);
  }
  
  private static class SEQReadMapper extends Mapper<Text, Text, Text, Text> {  
	  public void map(Text key, Text value, Context context) 
	  throws IOException, InterruptedException{		  
		  context.write(key, value);
	  }
  }  
}
