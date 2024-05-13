/**
 * Copyright (c) 2014 Daniele Pantaleone <danielepantaleone@icloud.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 * 
 * @author Daniele Pantaleone
 * @version 1.0
 * @copyright Daniele Pantaleone, 17 October, 2014
 * @package it.uniroma1.hadoop.pagerank
 */

package org.bigdatalab;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.bigdatalab.job1.PageRankJob1Mapper;
import org.bigdatalab.job1.PageRankJob1Reducer;
import org.bigdatalab.job2.PageRankJob2Mapper;
import org.bigdatalab.job2.PageRankJob2Reducer;
import org.bigdatalab.job3.PageRankJob3Mapper;


public class PageRank {

    public static double NUMBER_OF_LINKS = 0;
    public static String IN_PATH = "";
    public static String OUT_PATH = "";

    public static void main(String[] args) throws Exception {
        PageRank.IN_PATH = args[0];
        PageRank.OUT_PATH = args[1];
        PageRank.NUMBER_OF_LINKS = Double.parseDouble(args[2]);
        String inPath = null;
        String lastOutPath = null;
        PageRank pagerank = new PageRank();
        boolean isCompleted = pagerank.job1(IN_PATH, OUT_PATH + "/job1");
        if (!isCompleted) {
            System.exit(1);
        }
        inPath = OUT_PATH + "/job1";
        lastOutPath = OUT_PATH + "/job2";
        isCompleted = pagerank.job2(inPath, lastOutPath);
        if (!isCompleted) {
            System.exit(1);
        }
        isCompleted = pagerank.job3(lastOutPath, OUT_PATH + "/job3");
        if (!isCompleted) {
            System.exit(1);
        }
        System.exit(0);
    }

    public boolean job1(String in, String out) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration(), "Job #1");
        job.setJarByClass(PageRank.class);
        FileInputFormat.addInputPath(job, new Path(in));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(PageRankJob1Mapper.class);
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(PageRankJob1Reducer.class);
        job.getConfiguration().setDouble("numberOfLinks", NUMBER_OF_LINKS);
        return job.waitForCompletion(true);
    }
    

    public boolean job2(String in, String out) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration(), "Job #2");
        job.setJarByClass(PageRank.class);
        FileInputFormat.setInputPaths(job, new Path(in));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(PageRankJob2Mapper.class);
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(PageRankJob2Reducer.class);
        job.getConfiguration().setDouble("numberOfLinks", NUMBER_OF_LINKS);
        return job.waitForCompletion(true);
    }

    public boolean job3(String in, String out) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration(), "Job #3");
        job.setJarByClass(PageRank.class);
        FileInputFormat.setInputPaths(job, new Path(in));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(PageRankJob3Mapper.class);
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        job.setSortComparatorClass(RangSorter.class);
        return job.waitForCompletion(true);
        
    }
    
}