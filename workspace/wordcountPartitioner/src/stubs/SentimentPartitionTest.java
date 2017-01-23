package stubs;

//import static org.junit.Assert.assertEquals;
import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

public class SentimentPartitionTest {

	SentimentPartitioner mpart;

	@Test
	public void testSentimentPartition() {

		SentimentPartitioner spart = new SentimentPartitioner();
		spart.setConf(new Configuration());
		
		/*
		 * Test the words "love", "deadly", and "zodiac". 
		 * The expected outcomes should be 0, 1, and 2. 
		 */
		Assert.assertEquals(0, 
				spart.getPartition(new Text("love"), new IntWritable(), 3));
		System.out.println("Outcome for love: " 
		 + spart.getPartition(new Text("love"), new IntWritable(), 3));
		
		Assert.assertEquals(1, 
				spart.getPartition(new Text("deadly"), new IntWritable(), 3));
		System.out.println("Outcome for deadly: " 
		 + spart.getPartition(new Text("deadly"), new IntWritable(), 3));
		
		Assert.assertEquals(2, 
				spart.getPartition(new Text("zodiac"), new IntWritable(), 3));
		System.out.println("Outcome for zodiac: " + 
		 spart.getPartition(new Text("zodiac"), new IntWritable(), 3)); 	
	}
}
