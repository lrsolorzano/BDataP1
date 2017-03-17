package BDataP1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.IOException;

/**
 * Created by lsolorzano on 3/17/2017.
 */
public class TwitterKeyWordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String rawTweet = value.toString();

        try {
            Status status = TwitterObjectFactory.createStatus(rawTweet);
            String tweet = status.getText().toUpperCase();
            if (tweet.contains("MAGA")){
                context.write(new Text("MAGA"), new IntWritable(1));
            }
            else if (tweet.contains("DICTATOR")){
                context.write(new Text("DICTATOR"), new IntWritable(1));
            }
        }
        catch(TwitterException e){

        }

    }
}