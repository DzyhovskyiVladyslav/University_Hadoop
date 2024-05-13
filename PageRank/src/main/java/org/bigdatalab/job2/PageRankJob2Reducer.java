package org.bigdatalab.job2;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankJob2Reducer extends Reducer<Text, Text, Text, Text> {
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String links = "";
        double sumShareOtherPageRanks = 0.0;
        for (Text value : values) {
            String content = value.toString();
            if (content.startsWith("|")) {
                links += content.substring("|".length());
            } else {
                String[] split = content.split("\\t");
                double pageRank = Double.parseDouble(split[0]);
                int totalLinks = Integer.parseInt(split[1]);
                sumShareOtherPageRanks += (pageRank / totalLinks);
            }
        }
        double newRank = 0.15/context.getConfiguration().getDouble("numberOfLinks", 1) + 0.85 * sumShareOtherPageRanks;
        context.write(key, new Text(newRank + "\t" + links));
    }
}
