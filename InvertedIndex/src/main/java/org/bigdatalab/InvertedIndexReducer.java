package org.bigdatalab;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class InvertedIndexReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        StringBuilder sb = new StringBuilder();
        while (values.hasNext()) {
            Text value = values.next();
            sb.append(value);
            sb.append(", ");
        }
        sb.delete(sb.length() - 3, sb.length() - 1);
        output.collect(key, new Text(sb.toString()));
    }
}
