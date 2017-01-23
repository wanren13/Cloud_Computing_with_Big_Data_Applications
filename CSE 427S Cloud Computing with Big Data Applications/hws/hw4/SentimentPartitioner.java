package stubs;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class SentimentPartitioner extends Partitioner<Text, IntWritable> implements Configurable {

	private Configuration configuration;
	private static Set<String> positive = new HashSet<String>();
	private static Set<String> negative = new HashSet<String>();

	/**
	 * Set up the positive and negative hash set in the setConf method.
	 */
	@Override
	public void setConf(Configuration configuration) {
		/*
		 * Add the positive and negative words to the respective sets using the
		 * files positive-words.txt and negative-words.txt.
		 */
		/*
		 * TODO implement
		 */

		readFile((HashSet<String>) positive, "positive-words.txt");
		readFile((HashSet<String>) negative, "negative-words.txt");
	}

	/**
	 * Implement the getConf method for the Configurable interface.
	 */
	@Override
	public Configuration getConf() {
		return configuration;
	}

	/**
	 * You must implement the getPartition method for a partitioner class. This
	 * method receives the words as keys (i.e., the output key from the mapper.)
	 * It should return an integer representation of the sentiment category
	 * (positive, negative, neutral).
	 * 
	 * For this partitioner to work, the job configuration must have been set so
	 * that there are exactly 3 reducers.
	 */
	public int getPartition(Text key, IntWritable value, int numReduceTasks) {
		/*
		 * TODO implement Change the return 0 statement below to return the
		 * number of the sentiment category; use 0 for positive words, 1 for
		 * negative words, and 2 for neutral words. Use the sets of positive and
		 * negative words to find out the sentiment.
		 *
		 * Hint: use positive.contains(key.toString()) and
		 * negative.contains(key.toString()) If a word appears in both lists
		 * assume it is positive. That is, once you found that a word is in the
		 * positive list you do not need to check if it is in the negative list.
		 */
		if (positive.contains(key.toString()))
			return 0;
		if (negative.contains(key.toString()))
			return 1;

		return 2;
	}

	private void readFile(HashSet<String> set, String filename) {
		BufferedReader br;
		try {
			br = new BufferedReader(new FileReader(filename));
			String line = br.readLine();

			while (line != null) {
				if (line.charAt(0) != ';') {
					set.add(line);
				}
				line = br.readLine();
			}
			br.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
