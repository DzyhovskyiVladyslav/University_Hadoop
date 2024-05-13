package org.bigdatalab;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.StringTokenizer;

public class InvertedIndexMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    private Text word = new Text();
    private Text offset = new Text();

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        FileSplit fileSplit = (FileSplit)reporter.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        String line = value.toString();
        StringTokenizer itr = new StringTokenizer(line.replaceAll("[^a-zA-Z0-9 ]", "").toLowerCase());
        while(itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            String set = fileName + "@" + key.toString();
            offset.set(set);
            output.collect(word, offset);
        }
    }
}
