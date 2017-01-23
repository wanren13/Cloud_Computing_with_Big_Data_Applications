package stubs;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class IndexMapper extends Mapper<Text, Text, Text, Text> {

	private String filename = "";
	// Create a Pattern object
    static private Pattern r = Pattern.compile("(\\w*[A-Za-z]+'?\\w*)");
//    static private Pattern line_num_r = Pattern.compile("(^\\d+)");
    private Matcher m;
    private Text word = new Text();
    private Text location = new Text();
	
  @Override
  public void map(Text key, Text value, Context context) throws IOException,
      InterruptedException {

    /*
     * TODO implement
     */
   
    m = r.matcher(value.toString());
    while (m.find()) {
    	word.set(m.group(0).toLowerCase());
    	location.set(filename + "@" + key.toString());
    	context.write(word, location);
    }    
  }
  
  protected void setup (Context context) {
	  filename = ((FileSplit) context.getInputSplit()).getPath().getName();
  }
}