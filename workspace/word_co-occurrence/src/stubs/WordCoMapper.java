package stubs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCoMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private String[] phrases;
	private String lastWord = null;
	private Text coWord = new Text();
	static private IntWritable one = new IntWritable(1);
	private Matcher m;
	static private Pattern r = Pattern.compile("(\\w*[A-Za-z]+'?\\w*)");
	static private Pattern rp = Pattern.compile("([.,;:!?\"()\\[\\]{}])[^.,;:!?\"()\\[\\]{}]*$");
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		/*
		 * TODO implement
		 */
		String line = value.toString().toLowerCase(Locale.ENGLISH);
		if (line.isEmpty()) {
			lastWord = null;
			return;
		}
		
		phrases = line.split("[.,;:!?\"()\\[\\]{}]");
		
		ArrayList<ArrayList<String>> wordsList = new ArrayList<ArrayList<String>>();	
		for (String ph : phrases) {
			ArrayList<String> words = new ArrayList<String>();
			m = r.matcher(ph);
		    while (m.find()) {
		    	String word = m.group(0);
		    	if(!word.isEmpty()) {
		    		words.add(word);
		    	}
		    }
		    if(!words.isEmpty()) {
		    	wordsList.add(words);
		    }
		}
		
		if(wordsList.isEmpty()) {
			lastWord = null;
			return;
		}
		
		if (lastWord != null) {
			coWord.set(lastWord + "," + wordsList.get(0).get(0));
			context.write(coWord, one);
		}
		
		for (ArrayList<String> words : wordsList) {
			for (int i = 0; i < words.size() - 1; i++) {
				coWord.set(words.get(i) + "," + words.get(i + 1));
				context.write(coWord, one);
			}
		}
		
		ArrayList<String> lastPhrase = wordsList.get(wordsList.size() - 1);
		lastWord = lastPhrase.get(lastPhrase.size() - 1);
		int lastIndexOfWord = line.lastIndexOf(lastWord);
		
		m = rp.matcher(line);
		int lastIndexOfPunctuation = m.find()?m.start():-1;
		
		if (lastIndexOfPunctuation > lastIndexOfWord) {
			lastWord = null;
		}
		
	}
}
