package stubs;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

/**
 * On input, the reducer receives a word as the key and a set
 * of locations in the form "play name@line number" for the values. 
 * The reducer builds a readable string in the valueList variable that
 * contains an index of all the locations of the word. 
 */
public class IndexReducer extends Reducer<Text, Text, Text, Text> {

	private Text locText = new Text();
	boolean firstWord;
  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    /*
     * TODO implement
     */
	  String locStr = "";
	  firstWord = true;
	  
	  for (Text loc:values) {
		  if(firstWord) {
			  firstWord = false;
			  locStr = loc.toString();
		  }
		  else {
			  locStr += ("," + loc.toString());
		  }
	  }
	  
	  locText.set(locStr);
	  
    context.write(key, locText);
  }
}