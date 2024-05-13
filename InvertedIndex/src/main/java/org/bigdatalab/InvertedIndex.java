package org.bigdatalab;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

public class InvertedIndex {

    public static void main(String[] args) throws IOException {
        JobClient client = new JobClient();
        JobConf conf = new JobConf(InvertedIndex.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        conf.setMapperClass(InvertedIndexMapper.class);
        conf.setReducerClass(InvertedIndexReducer.class);
        conf.setCombinerClass(InvertedIndexReducer.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        client.setConf(conf);
        JobClient.runJob(conf);
    }
}