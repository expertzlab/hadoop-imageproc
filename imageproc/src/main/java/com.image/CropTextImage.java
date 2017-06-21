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
import org.hipi.image.FloatImage;
import org.hipi.image.HipiImageHeader;
import org.hipi.imagebundle.mapreduce.HibInputFormat;

import java.io.IOException;


/**
 * Created by gireeshbabu on 15/06/17.
 */
public class CropTextImage extends Configured implements Tool {

    String seedPointsStr; //in the format x1,y1:x2,y2:,x2,y3

    public int run(String[] args) throws Exception {
        // Check input arguments
        if (args.length < 2) {
            System.out.println("Usage: AverageImage <input HIB> <output directory>");
            System.exit(0);
        }
        // Initialize and configure MapReduce job
        Job job = Job.getInstance();
        // Set input format class which parses the input HIB and spawns map tasks
        //job.setInputFormatClass(FileInputFormat.class);
        // Set the driver, mapper, and reducer classes which express the computation
        job.setJarByClass(CropTextImage.class);
        job.setMapperClass(ImageMapper.class);
        job.setReducerClass(ImageReducer.class);
        // Set the types for the key/value pairs passed to/from map and reduce layers
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set the input and output paths on the HDFS
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //seedPointsStr = args[2];

        // Execute the MapReduce job and block until it complets
        boolean success = job.waitForCompletion(true);

        // Return success or failure
        return success ? 0 : 1;
    }

    public static class ImageMapper extends Mapper<LongWritable, Text, Text, Text>{

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

                        String strvalue = value.toString();
                        String[] splitArray = strvalue.split(",");
                        String cord = String.format("%3d,%3d,",splitArray[0],splitArray[1]);
                        context.write(new Text(cord), new Text(splitArray[2]));

        }


    public float calculateConnectedValue(float ic, float id){

            //Formula to calculate fuzzy connected value
            float s1 = 1;
            float m1 = 1;
            float w1 = 0.5f ;
            float w2 = 0.5f;
            float m2 = 1;

            //double g1 = Math.exp((0.5*(ic+id) - m1) * ((ic+id) - m1));
            //double g2 = Math.exp((0.5* (ic-id) - m1) * ((ic-id) - m1));
            double g1 = Math.random();
            double g2 = Math.random();
            return (float) ((float) w1 * g1 + w2 * g2);

        }

    }

    public static class ImageReducer extends Reducer<Text, Text, Text, Text>{

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Create FloatImage object to hold final result


            // Initialize a counter and iterate over IntWritable/FloatImage records from mapper
            float threshold = 0.5f;
            for (Text val : values) {

                //if ( val.getData()[1] > threshold ) {

                    //String result = String.format("Average pixel value: %f %f %f", avgData[0], avgData[1], avgData[2]);
                    // Emit output of job which will be written to HDFS
                    context.write(key, val);
                //}
            }

       } // reduce()
    }


    public static void main(String[] args) throws Exception {
        ToolRunner.run(new CropTextImage(), args);
        System.exit(0);
    }

}