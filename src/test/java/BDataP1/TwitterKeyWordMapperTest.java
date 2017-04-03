package BDataP1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

/**
 * Created by lsolorzano on 3/21/2017.
 */
public class TwitterKeyWordMapperTest {
    @Test
    public void processMapRecord() throws IOException, InterruptedException{
        Text row = new Text("{      \"text\": \"RT @PostGradProblem: In preparation for the NFL lockout, I will be spending twice as much time analyzing my fantasy baseball team during ...\", \n" +
                "      \"truncated\": true, \n" +
                "      \"in_reply_to_user_id\": null, \n" +
                "      \"in_reply_to_status_id\": null, \n" +
                "      \"favorited\": false, \n" +
                "      \"source\": \"<a href=\\\"http://twitter.com/\\\" rel=\\\"nofollow\\\">Twitter for iPhone</a>\", \n" +
                "      \"in_reply_to_screen_name\": null, \n" +
                "      \"in_reply_to_status_id_str\": null, \n" +
                "      \"id_str\": \"54691802283900928\", \n" +
                "      \"entities\": {\n" +
                "            \"user_mentions\": [\n" +
                "                  {\n" +
                "                        \"indices\": [\n" +
                "                              3, \n" +
                "                              19\n" +
                "                        ], \n" +
                "                        \"screen_name\": \"PostGradProblem\", \n" +
                "                        \"id_str\": \"271572434\", \n" +
                "                        \"name\": \"PostGradProblems\", \n" +
                "                        \"id\": 271572434\n" +
                "                  }\n" +
                "            ], \n" +
                "            \"urls\": [ ], \n" +
                "            \"hashtags\": [ ]\n" +
                "      }, \n" +
                "      \"contributors\": null, \n" +
                "      \"retweeted\": false, \n" +
                "      \"in_reply_to_user_id_str\": null, \n" +
                "      \"place\": null, \n" +
                "      \"retweet_count\": 4, \n" +
                "      \"created_at\": \"Sun Apr 03 23:48:36 +0000 2011\", \n" +
                "      \"retweeted_status\": {\n" +
                "            \"text\": \"In preparation for the NFL lockout, I will be spending twice as much time analyzing my fantasy baseball team during company time. #PGP\", \n" +
                "            \"truncated\": false, \n" +
                "            \"in_reply_to_user_id\": null, \n" +
                "            \"in_reply_to_status_id\": null, \n" +
                "            \"favorited\": false, \n" +
                "            \"source\": \"<a href=\\\"http://www.hootsuite.com\\\" rel=\\\"nofollow\\\">HootSuite</a>\", \n" +
                "            \"in_reply_to_screen_name\": null, \n" +
                "            \"in_reply_to_status_id_str\": null, \n" +
                "            \"id_str\": \"54640519019642881\", \n" +
                "            \"entities\": {\n" +
                "                  \"user_mentions\": [ ], \n" +
                "                  \"urls\": [ ], \n" +
                "                  \"hashtags\": [\n" +
                "                        {\n" +
                "                              \"text\": \"PGP\", \n" +
                "                              \"indices\": [\n" +
                "                                    130, \n" +
                "                                    134\n" +
                "                              ]\n" +
                "                        }\n" +
                "                  ]\n" +
                "            }, \n" +
                "            \"contributors\": null, \n" +
                "            \"retweeted\": false, \n" +
                "            \"in_reply_to_user_id_str\": null, \n" +
                "            \"place\": null, \n" +
                "            \"retweet_count\": 4, \n" +
                "            \"created_at\": \"Sun Apr 03 20:24:49 +0000 2011\", \n" +
                "            \"user\": {\n" +
                "                  \"notifications\": null, \n" +
                "                  \"profile_use_background_image\": true, \n" +
                "                  \"statuses_count\": 31, \n" +
                "                  \"profile_background_color\": \"C0DEED\", \n" +
                "                  \"followers_count\": 3066, \n" +
                "                  \"profile_image_url\": \"http://a2.twimg.com/profile_images/1285770264/PGP_normal.jpg\", \n" +
                "                  \"listed_count\": 6, \n" +
                "                  \"profile_background_image_url\": \"http://a3.twimg.com/a/1301071706/images/themes/theme1/bg.png\", \n" +
                "                  \"description\": \"\", \n" +
                "                  \"screen_name\": \"PostGradProblem\", \n" +
                "                  \"default_profile\": true, \n" +
                "                  \"verified\": false, \n" +
                "                  \"time_zone\": null, \n" +
                "                  \"profile_text_color\": \"333333\", \n" +
                "                  \"is_translator\": false, \n" +
                "                  \"profile_sidebar_fill_color\": \"DDEEF6\", \n" +
                "                  \"location\": \"\", \n" +
                "                  \"id_str\": \"271572434\", \n" +
                "                  \"default_profile_image\": false, \n" +
                "                  \"profile_background_tile\": false, \n" +
                "                  \"lang\": \"en\", \n" +
                "                  \"friends_count\": 21, \n" +
                "                  \"protected\": false, \n" +
                "                  \"favourites_count\": 0, \n" +
                "                  \"created_at\": \"Thu Mar 24 19:45:44 +0000 2011\", \n" +
                "                  \"profile_link_color\": \"0084B4\", \n" +
                "                  \"name\": \"PostGradProblems\", \n" +
                "                  \"show_all_inline_media\": false, \n" +
                "                  \"follow_request_sent\": null, \n" +
                "                  \"geo_enabled\": false, \n" +
                "                  \"profile_sidebar_border_color\": \"C0DEED\", \n" +
                "                  \"url\": null, \n" +
                "                  \"id\": 271572434, \n" +
                "                  \"contributors_enabled\": false, \n" +
                "                  \"following\": null, \n" +
                "                  \"utc_offset\": null\n" +
                "            }, \n" +
                "            \"id\": 54640519019642880, \n" +
                "            \"coordinates\": null, \n" +
                "            \"geo\": null\n" +
                "      }, \n" +
                "      \"user\": {\n" +
                "            \"notifications\": null, \n" +
                "            \"profile_use_background_image\": true, \n" +
                "            \"statuses_count\": 351, \n" +
                "            \"profile_background_color\": \"C0DEED\", \n" +
                "            \"followers_count\": 48, \n" +
                "            \"profile_image_url\": \"http://a1.twimg.com/profile_images/455128973/gCsVUnofNqqyd6tdOGevROvko1_500_normal.jpg\", \n" +
                "            \"listed_count\": 0, \n" +
                "            \"profile_background_image_url\": \"http://a3.twimg.com/a/1300479984/images/themes/theme1/bg.png\", \n" +
                "            \"description\": \"watcha doin in my waters?\", \n" +
                "            \"screen_name\": \"OldGREG85\", \n" +
                "            \"default_profile\": true, \n" +
                "            \"verified\": false, \n" +
                "            \"time_zone\": \"Hawaii\", \n" +
                "            \"profile_text_color\": \"333333\", \n" +
                "            \"is_translator\": false, \n" +
                "            \"profile_sidebar_fill_color\": \"DDEEF6\", \n" +
                "            \"location\": \"Texas\", \n" +
                "            \"id_str\": \"80177619\", \n" +
                "            \"default_profile_image\": false, \n" +
                "            \"profile_background_tile\": false, \n" +
                "            \"lang\": \"en\", \n" +
                "            \"friends_count\": 81, \n" +
                "            \"protected\": false, \n" +
                "            \"favourites_count\": 0, \n" +
                "            \"created_at\": \"Tue Oct 06 01:13:17 +0000 2009\", \n" +
                "            \"profile_link_color\": \"0084B4\", \n" +
                "            \"name\": \"GG\", \n" +
                "            \"show_all_inline_media\": false, \n" +
                "            \"follow_request_sent\": null, \n" +
                "            \"geo_enabled\": false, \n" +
                "            \"profile_sidebar_border_color\": \"C0DEED\", \n" +
                "            \"url\": null, \n" +
                "            \"id\": 80177619, \n" +
                "            \"contributors_enabled\": false, \n" +
                "            \"following\": null, \n" +
                "            \"utc_offset\": -36000\n" +
                "      }, \n" +
                "      \"id\": 54691802283900930, \n" +
                "      \"coordinates\": null, \n" +
                "      \"geo\": null }");
                new MapDriver<LongWritable, Text, Text,IntWritable>()
                .withMapper(new TwitterKeyWordCountMapper())
                .withInput(new LongWritable(0), row)
                .withOutput(new Text("TEST"), new IntWritable(1))
                .runTest();
    }

    @Test
    public void processReducedRecord() throws IOException, InterruptedException {
        Text key = new Text("TRUMP");

        new ReduceDriver<Text, IntWritable, Text, IntWritable>()
                .withReducer(new TwitterKeyWorkReducer())
                .withInput(key, Arrays.asList(new IntWritable(1), new IntWritable(1), new IntWritable(1),
                        new IntWritable(1)))
                .withOutput(key, new IntWritable(4))
                .runTest();
    }
}