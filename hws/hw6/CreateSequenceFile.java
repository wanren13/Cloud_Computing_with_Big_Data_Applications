package stubs;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class CreateSequenceFile extends Configured implements Tool {

@Override
  public int run(String[] args) throws Exception {

    if (args.length != 2) {
      System.out.printf("Usage: CreateSequenceFile <input dir> <output dir>\n");
      return -1;
    }

    Configuration conf = getConf();
    
    Job job = Job.getInstance(conf);
    job.setJarByClass(CreateSequenceFile.class);
    job.setJobName("Create Sequence File");

    /*
     * TODO implement
     */
    
    Path inPath = new Path(args[0]);
    Path outPath = new Path(args[1]);
    
	FileInputFormat.setInputPaths(job,  inPath);
	FileOutputFormat.setOutputPath(job, outPath);
	
	job.setMapperClass(SEQMapper.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	job.setNumReduceTasks(0);
	
	SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
	job.setOutputFormatClass(SequenceFileOutputFormat.class);
	
	FileOutputFormat.setCompressOutput(job, true);
	FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);	
	
    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Configuration(), new CreateSequenceFile(), args);
    System.exit(exitCode);
  }
  
  public static class SEQMapper extends Mapper<LongWritable, Text, Text, Text> {
	  private static Text seqkey = new Text();
	  private static int mapperCount = 0;
	  private int mapperId = 0;
	  public void map(LongWritable key, Text value, Context context) 
	  throws IOException, InterruptedException{
		  
		  seqkey.set(key.get() + " | " + mapperId + ">>");
		  context.write(seqkey, value);
	  }
	  
	  protected void Setup (Context contex) {
		  mapperId = ++mapperCount;
	  }	  
  }  
}
