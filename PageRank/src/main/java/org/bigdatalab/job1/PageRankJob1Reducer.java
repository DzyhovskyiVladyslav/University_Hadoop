package org.bigdatalab.job1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

public class PageRankJob1Reducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        boolean first = true;
        String links = 1.0/context.getConfiguration().getDouble("numberOfLinks", 1) + "\t";
        for (Text value : values) {
            if (!first)
                links += ",";
            links += value.toString();
            first = false;
        }
        context.write(key, new Text(links));
    }
}
