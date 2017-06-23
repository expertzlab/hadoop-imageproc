package com.image;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


/**
 * Created by gireeshbabu on 15/06/17.
 */
public class CropTextImage extends Configured implements Tool {


    public int run(String[] args) throws Exception {
        // Check input arguments
        if (args.length < 2) {
            System.out.println("Usage: CropTextImage <input HIB> <output directory>");
            System.exit(0);
        }

        boolean success = MRMean1Job.triggerJobAndWait(args[0],args[1]);


        // Return success or failure
        return success ? 0 : 1;
    }


    public static void main(String[] args) throws Exception {
        ToolRunner.run(new CropTextImage(), args);
        System.exit(0);
    }

}