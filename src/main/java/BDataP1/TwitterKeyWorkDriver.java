package BDataP1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by lsolorzano on 3/17/2017.
 */
public class TwitterKeyWorkDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: TwitterKeyWorkDriver <input path> <output path>");
            System.exit(-1);
        }
        Job job = new Job();
        job.setJarByClass(TwitterKeyWorkDriver.class);
        job.setJobName("Count TweetsbyWord");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(TwitterKeyWordCountMapper.class);
        job.setReducerClass(BDataP1.TwitterKeyWorkReducer.class);
        //job.setCombinerClass(edu.uprm.cse.bigdata.mrsp02.TwitterReduceByScreenName.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}