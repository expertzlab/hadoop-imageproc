package com.image;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
public class CropImage extends Configured implements Tool {

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
        job.setInputFormatClass(HibInputFormat.class);
        // Set the driver, mapper, and reducer classes which express the computation
        job.setJarByClass(CropImage.class);
        job.setMapperClass(ImageMapper.class);
        job.setReducerClass(ImageReducer.class);
        // Set the types for the key/value pairs passed to/from map and reduce layers
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatImage.class);
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

    public static class ImageMapper extends Mapper<HipiImageHeader, FloatImage, Text, FloatImage>{

        public void map(HipiImageHeader key, FloatImage value, Context context) throws IOException, InterruptedException {
        // Verify that image was properly decoded, is of sufficient size, and has three color channels (RGB)
            if (value != null && value.getWidth() > 1 && value.getHeight() > 1 && value.getNumBands() == 3) {


                // Get dimensions of image
                int w = value.getWidth();
                int h = value.getHeight();
                long size = w * h;

                int seedIntensity = 100;

                // Get pointer to image data
                float[] valData = value.getData();

                float xyl = 0;
                float xyr = 0;
                float xyu = 0;
                float xyd = 0;
                float[] intensity = new float[2];
                FloatImage extractedImage = null;

                // Traverse image pixel data in raster-scan order and update running average
                for (int j = 0; j < h; j++) {
                    for (int i = 0; i < w; i++) {

                        String cord = String.format("%3d,%3d,",i,j);
                        //write image as it is
                        xyl = valData[(j*w+i)];
                        intensity[0] = xyl;// xyl;
                        intensity[1] = xyl;//calculateConnectedValue(xyl, seedIntensity);
                        extractedImage = new FloatImage(1, 1, 2, intensity);
                        context.write(new Text(cord), extractedImage);
                        /*
                        if(((j*w+i)-1)> 0){
                            xyl = valData[(j*w+i)-1];
                            intensity[0] = 5;// xyl;
                            intensity[1] = xyl;//calculateConnectedValue(xyl, seedIntensity);
                            // Create a FloatImage to store the average value
                            extractedImage = new FloatImage(1, 1, 2, intensity);
                            context.write(new Text(cord), extractedImage);
                        }

                        if(((j*w+i)+1) < size){
                            xyr = valData[(j*w+i)+1];
                            intensity[0] = 5;// xyr;
                            intensity[1] = xyr; //calculateConnectedValue(xyr, seedIntensity);
                            // Create a FloatImage to store the average value
                            extractedImage = new FloatImage(1, 1, 2, intensity);
                            context.write(new Text(cord), extractedImage);
                        }
                        if(((j*w+i)-w) > 0){
                            xyu = valData[(j*w+i)-w];
                            intensity[0] = 5;//xyu;
                            intensity[1] = xyu;//calculateConnectedValue(xyu, seedIntensity);
                            // Create a FloatImage to store the average value
                            extractedImage = new FloatImage(1, 1, 2, intensity);
                            context.write(new Text(cord), extractedImage);
                        }

                        if(((j*w+i) + w) < size) {
                            xyd = valData[(j * w + i) + w];
                            intensity[0] = 5;//xyd;
                            intensity[1] = xyd;//calculateConnectedValue(xyd, seedIntensity);
                            // Create a FloatImage to store the average value
                            extractedImage = new FloatImage(1, 1, 2, intensity);
                            context.write(new Text(cord), extractedImage);
                        }
                        */

                    }
                }
            } // If (value != null...
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

    public static class ImageReducer extends Reducer<Text, FloatImage, Text, Text>{

        public void reduce(Text key, Iterable<FloatImage> values, Context context) throws IOException, InterruptedException {
        // Create FloatImage object to hold final result


            // Initialize a counter and iterate over IntWritable/FloatImage records from mapper
            float threshold = 0.5f;
            for (FloatImage val : values) {

                //if ( val.getData()[1] > threshold ) {

                    //String result = String.format("Average pixel value: %f %f %f", avgData[0], avgData[1], avgData[2]);
                    // Emit output of job which will be written to HDFS
                    context.write(key, new Text( String.valueOf(val.getData()[0])));
                //}
            }

       } // reduce()
    }


    public static void main(String[] args) throws Exception {
        ToolRunner.run(new CropImage(), args);
        System.exit(0);
    }

}