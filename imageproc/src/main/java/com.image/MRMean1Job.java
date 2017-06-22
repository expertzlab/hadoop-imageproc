package com.image;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by gireeshbabu on 21/06/17.
 */
public class MRMean1Job {


    static public boolean triggerJobAndWait(String inputPath, String outPath) throws IOException, InterruptedException, ClassNotFoundException {

        // Initialize and configure MapReduce job
        Job job = Job.getInstance();
        // Set input format class which parses the input HIB and spawns map tasks
        //job.setInputFormatClass(FileInputFormat.class);
        // Set the driver, mapper, and reducer classes which express the computation
        job.setJarByClass(CropTextImage.class);
        job.setMapperClass(MRMean1Job.ImageMapper.class);
        job.setReducerClass(MRMean1Job.ImageReducer.class);
        // Set the types for the key/value pairs passed to/from map and reduce layers
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set the input and output paths on the HDFS
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));

        // Execute the MapReduce job and block until it complets
        return job.waitForCompletion(true);
    }


    public static class ImageMapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String strValue = value.toString();
            String[] splitEachLineRecArray = strValue.split(",");
            if (splitEachLineRecArray.length < 3)
                return;
            writeOutPixelsForEachSeedPoint(context, splitEachLineRecArray);

        }

        public static void writeOutPixelsForEachSeedPoint(Mapper<LongWritable, Text, Text, Text>.Context context, String[] splitEachLineRecArray) throws IOException, InterruptedException {

            String[] seedPointsArray = "50,50,50;100,50,50;100,100,50".split(";");

            String x = splitEachLineRecArray[0];
            String y = splitEachLineRecArray[1];
            String intensity = splitEachLineRecArray[2];

            for (String seedPoint : seedPointsArray) {
                String[] seedValues = seedPoint.split(",");
                String keyOut = String.format("%s,%s", seedValues[0], seedValues[1]);
                String valueOut = String.format("%s,%s,%s,%s", x, y, intensity,seedValues[2]);
                context.write(new Text(keyOut), new Text(valueOut));
            }
        }

    }



    public static class ImageReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String[] seedPoint = key.toString().split(",");
            int seedx = Integer.parseInt(seedPoint[0]);
            int seedy = Integer.parseInt(seedPoint[1]);

            //mean of sum
            float[] meanResults = getIntesityMeanOfSumAndDiff(values);
            float m1 = meanResults[0];
            float m2 = meanResults[1];
            float[] sdResults = getIntnsityDeviationsofSumAndDiff(meanResults, values);
            float s1 = sdResults[0];
            float s2 = sdResults[0];

            reduceOnConnectedValue(key, values, context, seedPoint, m1, m2, s1, s2);

        }

        public static void reduceOnConnectedValue(Text seedKey, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context, String[] seedPoint, float m1, float m2, float s1, float s2) throws IOException, InterruptedException {

            HashMap<String,Integer> pixelValuesMap = new HashMap<>();
            for (Text val : values) {
                String[] pixelVal = val.toString().split(",");
                String xyCoord = pixelVal[0]+","+pixelVal[1];
                int pixelIntensity = Integer.parseInt(pixelVal[2]);
                pixelValuesMap.put(xyCoord,pixelIntensity);
            }

            int seedx = Integer.parseInt( seedPoint[0]);
            int seedy = Integer.parseInt( seedPoint[1]);
            int seedIntensity = Integer.parseInt( seedPoint[2]);
            String nextKey = seedKey.toString();
            while(pixelValuesMap.get(nextKey) != null) {
                context.write(new Text(nextKey), new Text( String.valueOf(seedIntensity)));
                pixelValuesMap.put(seedKey.toString(),null);
                nextKey = findNextPoint(seedx, seedy, pixelValuesMap, m1, m2, s1, s2);
            }
        }

        private static String findNextPoint(int seedx, int seedy, Map pixelValuesMap, float m1, float m2, float s1, float s2) throws IOException, InterruptedException {

            String seedkey = ""+seedx+","+seedy;


            float connectedValueTemp =0;

            String xyl = ""+(seedx-1)+","+(seedy);
            String nextKey = xyl;
            float connectedValueMin = calcConnectedValueTo(seedkey, xyl, pixelValuesMap, m1, m2, s1, s2);

            String xyr = ""+(seedx+1)+","+(seedy);
            connectedValueTemp = calcConnectedValueTo(seedkey, xyl, pixelValuesMap, m1, m2, s1, s2);
            if(connectedValueTemp < connectedValueMin){
                nextKey = xyr;
                connectedValueMin = connectedValueTemp;
            }


            String xyu = ""+(seedx)+","+(seedy-1);
            connectedValueTemp = calcConnectedValueTo(seedkey, xyl, pixelValuesMap, m1, m2, s1, s2);
            if(connectedValueTemp < connectedValueMin){
                nextKey = xyu;
                connectedValueMin = connectedValueTemp;
            }

            String xyd = ""+(seedx)+","+(seedy+1);
            connectedValueTemp = calcConnectedValueTo(seedkey, xyl, pixelValuesMap, m1, m2, s1, s2);
            if(connectedValueTemp < connectedValueMin){
                nextKey = xyd;
            }
            return nextKey;
        }

        public static float calcConnectedValueTo(String seedkey, String xyl, Map pixelValuesMap, float m1, float m2, float s1, float s2) throws IOException, InterruptedException {

            int seedIntensity = (int) pixelValuesMap.get(seedkey);
            int pixelIntensity = (int) pixelValuesMap.get(xyl);
            double g1 = Math.exp(Math.pow((((0.5*(seedIntensity + pixelIntensity))- m1)/s1),2)/2);
            double g2 = Math.exp(Math.pow((((0.5*(seedIntensity - pixelIntensity))- m2)/s2),2)/2);
            float w1 = (float) (g1 / (g1+g2));
            float w2 = 1 - w1;
            float mu = (float) (w1 * g1 + w2 * g2);
            return mu;

        }


        private float[] getIntnsityDeviationsofSumAndDiff(float[] meanResults,Iterable<Text> values) {

            int size = 0;
            float m1 = meanResults[0];
            float m2 = meanResults[1];

            int deviationSum1 = 0;
            int deviationSum2 = 0;

            float[] sd = new float[2];

            for (Text val : values) {
                size = size + 1;
                String[] pixelVal = val.toString().split(",");
                int pixelIntensity = Integer.parseInt(pixelVal[2]);
                deviationSum1 += (deviationSum1 +  Math.pow((pixelIntensity - m1),2) );
                deviationSum2 += (deviationSum2 +  Math.pow((pixelIntensity - m2),2) );
            }
            sd[0] = (float) Math.sqrt(deviationSum1/size);
            sd[1] = (float) Math.sqrt(deviationSum2/size);
            return sd;
        }


        private static float[] getIntesityMeanOfSumAndDiff(Iterable<Text> values){

            int size = 0;
            long preMeanSumofIntensitySum = 0;
            long preMeanSumofIntensityDiff = 0;
            float[] results = new float[2];
            for (Text val : values) {
                size ++;
                String[] pixelVal = val.toString().split(",");
                int pixelIntensity = Integer.parseInt(pixelVal[2]);
                int seedIntensity = Integer.parseInt(pixelVal[3]);
                preMeanSumofIntensitySum += preMeanSumofIntensitySum + (0.5 *(seedIntensity + pixelIntensity));
                preMeanSumofIntensityDiff += preMeanSumofIntensityDiff + (0.5 *(seedIntensity - pixelIntensity));
            }
            results[0] = (preMeanSumofIntensitySum / size);
            results[1] =  (preMeanSumofIntensityDiff / size);
            return results;
        }

    }


    private static String[] parseSeedString(String seedStr){

        return seedStr.split(";");

    }

}
