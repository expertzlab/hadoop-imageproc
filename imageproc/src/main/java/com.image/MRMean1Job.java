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
import java.util.*;

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

        // Execute the MapReduce job and block until it complete
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

            String[] seedPointsArray = ("83,137,142;" +
                    "70,130,89;" +
                    "56,121,114;" +
                    "71,109,108;" +
                    "86,96,51;" +
                    "106,84,100;" +
                    "121,79,41;" +
                    "117,92,159;" +
                    "104,106,182;" +
                    "95,116,113;" +
                    "89,126,141;" +
                    "81,135,20;" +
                    "72,127,155").split(";");

            String x = splitEachLineRecArray[0];
            String y = splitEachLineRecArray[1];
            String intensity = splitEachLineRecArray[2];

            for (String seedPoint : seedPointsArray) {
                String[] seedValues = seedPoint.split(",");
                String keyOut = String.format("%s,%s", seedValues[0], seedValues[1]);
                String valueOut = String.format("%s,%s,%s,%s;", x, y, intensity,seedValues[2]);
                context.write(new Text(keyOut), new Text(valueOut));
            }
        }

    }



    public static class ImageReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String[] seedPoint = key.toString().split(",");
            int seedx = Integer.parseInt(seedPoint[0]);
            int seedy = Integer.parseInt(seedPoint[1]);

            List<String> xyCoordinates = new ArrayList();
            int w = 1;
            int h = 1;
            for (Text val : values) {
                String[] strings = val.toString().split(";");
                int x = Integer.parseInt(strings[0]);
                int y = Integer.parseInt(strings[1]);
                if(x > w){
                    w = x;
                }
                if(y > h){
                    h = y;
                }
                for(String str:strings) {
                    xyCoordinates.add(str);
                }
            }
            System.out.println("BEGIN : Size of values in reduce : "+ xyCoordinates.size());
            System.out.println("seed key:"+key);
            //mean of sum
            float[] meanResults = getIntesityMeanOfSumAndDiff(xyCoordinates);
            float m1 = meanResults[0];
            float m2 = meanResults[1];
            float[] sdResults = getIntensityDeviationsofSumAndDiff(meanResults, xyCoordinates);
            float s1 = sdResults[0];
            float s2 = sdResults[1];

            reduceOnConnectedValue(key, xyCoordinates, context, seedPoint, m1, m2, s1, s2, w, h);

        }

        public static void reduceOnConnectedValue(Text seedKey, List<String> xyIntensityList, Reducer<Text, Text, Text, Text>.Context context, String[] seedPoint, float m1, float m2, float s1, float s2, int w, int h) throws IOException, InterruptedException {

            /*
            HashMap<String,Integer> pixelValuesMap = new HashMap<>();
            for (String val : xyIntensityList) {
                String[] pixelVal = val.split(",");
                String xyCoord = pixelVal[0]+","+pixelVal[1];
                int pixelIntensity = Integer.parseInt(pixelVal[2]);
                pixelValuesMap.put(xyCoord,pixelIntensity);

            }
            */

            int[] pixelIntensityArray = convertToArray(xyIntensityList, w, h);


            int seedx = Integer.parseInt( seedPoint[0]);
            int seedy = Integer.parseInt( seedPoint[1]);
            int pixelSizeTobeAnalyzed = xyIntensityList.size() - 1;




            /*
            String nextKey = seedKey.toString();
            if(!pixelValuesMap.containsKey(nextKey)){
                throw new IllegalArgumentException("The seed intensity not obtained because of x,y value");
            }
            int pixelCount = pixelValuesMap.size();
            int count = pixelCount -1;
            while(count > 0) {
                count = count - 3;
                if(pixelValuesMap.get(nextKey) == null){
                    System.out.println("Next key returned :" +nextKey+", and the intensity is null and breaking");
                    break;
                }
                context.write(new Text(nextKey+","), new Text( pixelValuesMap.get(nextKey).toString()));
                System.out.println("Next key written: "+nextKey );
                String previousSeed = nextKey;

                String[] nextSeedPoint = nextKey.split(",");
                seedx = Integer.parseInt( nextSeedPoint[0]);
                seedy = Integer.parseInt( nextSeedPoint[1]);
                nextKey = findNextPoint(seedx, seedy, previousSeed, pixelValuesMap, m1, m2, s1, s2);
                pixelValuesMap.put(previousSeed,null);

                while(nextKey.length() != 0  && pixelValuesMap.get(nextKey) == null && count > 0 ) {
                    nextSeedPoint = nextKey.split(",");
                    seedx = Integer.parseInt( nextSeedPoint[0]);
                    seedy = Integer.parseInt( nextSeedPoint[1]);
                    nextKey = findNextPoint(seedx, seedy, previousSeed, pixelValuesMap, m1, m2, s1, s2);
                    System.out.println("In while - Next key which is null: "+nextKey );
                    previousSeed = seedx+","+seedy;
                }

            } */

        }

        private static void writeConnectedValueOfCombination(int r, int [][] pixelIntensityArray, int seedx, int seedy, int w, int h) {

            int n = w * h;

            HashMap <HashSet,String> combinationSet = new HashMap<>(fact(n)/(fact(r)*fact(n-r)));
            for(int i =0; i<n; i++){

            }
        }

        private static void calculateMeansAndSigmas()
        {

            //Will never add duplicates, since we use HashSets
            HashSet<Integer> spels = new HashSet<Integer>();
            for(int i : m_seeds)
            {
                int[] neighbors = getNeighbors(i);
                for(int j : neighbors)
                {
                    if(j == -1)
                        continue;

                    int[] neighborsNeighbors = getNeighbors(j);

                    for(int k : neighborsNeighbors)
                    {
                        if(k == -1)
                            continue;

                        spels.add(k);
                    }
                }
            }

            //Push all combinations of ave and reldiff to the arrays
            int numSpels = spels.size();

            //Weird java stuff to get all spels from the hasmap into an int array
            Integer[] temp = spels.toArray(new Integer[numSpels]);
            int[] spelsArray = new int[temp.length];
            for(int i = 0; i < numSpels; i++)
                spelsArray[i] = temp[i];

            int numCombinations = (numSpels * (numSpels - 1)) / 2;
            float[] aves = new float[numCombinations];
            float[] reldiffs = new float[numCombinations];

            int count = 0;
            for(int i = 0; i < numSpels - 1; i++)
            {
                for(int j = i+1; j < numSpels; j++)
                {
                    aves[count] = ave(spelsArray[i], spelsArray[j]);
                    reldiffs[count] = reldiff(spelsArray[i], spelsArray[j]);

                    count++;
                }
            }

            float[] ave_meanSigma = welford(aves);
            float[] reldiff_meanSigma = welford(reldiffs);

            m_mean_ave = ave_meanSigma[0];
            m_sigma_ave = ave_meanSigma[1];

            m_mean_reldiff = reldiff_meanSigma[0];
            m_sigma_reldiff = reldiff_meanSigma[1];

            System.out.println("ave mean: " + m_mean_ave);
            System.out.println("ave sigma: " + m_sigma_ave);
            System.out.println("reldiff mean: " + m_mean_reldiff);
            System.out.println("reldiff sigma: " + m_sigma_reldiff);
        }

        /**
         * Single-pass average and standard deviation calculation
         * https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm
         * @param elements list of elements to calculate the average and standard deviation of
         * @return length 2 float array with [average, standardDeviation]
         */
        private static float[] welford(float[] els)
        {
            int n = 0;
            double mean = 0;
            double M2 = 0;
            double var = 0;
            double delta = 0;

            for(float x : els)
            {
                n += 1;
                delta = x - mean;
                mean += delta / n;
                M2 += delta * (x - mean);
            }

            if(n < 2)
                return null;
            else
                var = M2 / (n - 1);

            float[] result = new float[2];
            result[0] = (float)mean;
            result[1] = (float)Math.sqrt(var);

            return result;
        }

        private static float gaussian(float val, float avg, float sigma)
        {
            return (float) Math.exp(-(1.0/(2*sigma*sigma)) * (val - avg) * (val - avg));
        }

        private static float affinity(int c, int d)
        {
            float g_ave = gaussian(ave(c, d), m_mean_ave, m_sigma_ave);
            float g_reldiff = gaussian(reldiff(c, d), m_mean_reldiff, m_sigma_reldiff);

            return Math.min(g_ave, g_reldiff);
        }

        private static float ave(int c, int d)
        {
            return 0.5f * ((float)m_imagePixels[c] + (float)m_imagePixels[d]);
        }

        private static float reldiff(int c, int d)
        {
            float fc = m_imagePixels[c];
            float fd = m_imagePixels[d];

            return fc == -fd ? 0 : (Math.abs(fc - fd)) / (fc + fd);
        }

        private static int[] getNeighbors(int c,int w, int h)
        {
            int m_width = w;
            int m_height = h;
            int m_depth = 1;
            int m_pixelsPerSlice = w * h;

            int z = c / m_pixelsPerSlice;
            int y = (c % m_pixelsPerSlice) / m_width;
            int x = (c % m_pixelsPerSlice) % m_width;

            int[] result = new int[6];
            result[0] = x < m_width-1 ? 			c + 1 : -1;
            result[1] = x > 0 ? 					c - 1 : -1;
            result[2] = y < m_height-1 ? 			c + m_width : -1;
            result[3] = y > 0 ? 					c - m_width : -1;
            result[4] = z < m_depth-1 ?			 	c + m_pixelsPerSlice : -1;
            result[5] = z > 0 ? 					c - m_pixelsPerSlice : -1;

            return result;
        }


        private static int fact(int n){
            return n * fact(n-1);
        }

        private static String findNextPoint(int seedx, int seedy, String previousSeed, Map pixelValuesMap, float m1, float m2, float s1, float s2) throws IOException, InterruptedException {

            String seedkey = ""+seedx+","+seedy;
            System.out.println("New seed key :"+ seedkey);

            float connectedValueTemp = 0;
            float connectedValueMin = 0.6f;
            String nextKey = "";

            String xyl = ""+(seedx-1)+","+(seedy);
            System.out.println("Trying on xyl:"+xyl+",intensity:"+pixelValuesMap.get(xyl));
            if(pixelValuesMap.get(xyl)!= null && !xyl.equals(previousSeed)) {
                nextKey = xyl;
                connectedValueMin = calcConnectedValueTo(seedkey, xyl, pixelValuesMap, m1, m2, s1, s2);
                System.out.println("xyl:"+xyl+", connected value :"+ connectedValueMin);
            }

            String xyr = ""+(seedx+1)+","+(seedy);
            System.out.println("Trying on xyr:"+xyr+",intensity:"+pixelValuesMap.get(xyr));
            if(pixelValuesMap.get(xyr)!= null && !xyl.equals(previousSeed) ) {
                connectedValueTemp = calcConnectedValueTo(seedkey, xyr, pixelValuesMap, m1, m2, s1, s2);
                System.out.println("xyr:"+xyr+", connected value :"+ connectedValueTemp);
                if (connectedValueTemp < connectedValueMin) {
                    nextKey = xyr;
                    connectedValueMin = connectedValueTemp;
                } else{
                    pixelValuesMap.put(xyr, null);
                }
            }


            String xyu = ""+(seedx)+","+(seedy-1);
            System.out.println("Trying on xyu: "+xyu+",intensity:"+pixelValuesMap.get(xyu));
            if(pixelValuesMap.get(xyu)!= null && !xyl.equals(previousSeed)) {
                connectedValueTemp = calcConnectedValueTo(seedkey, xyu, pixelValuesMap, m1, m2, s1, s2);
                System.out.println("xyu : "+xyu+", connected value :"+ connectedValueTemp);
                if(connectedValueTemp < connectedValueMin){
                    nextKey = xyu;
                    connectedValueMin = connectedValueTemp;
                }else{
                    pixelValuesMap.put(xyu, null);
                }
            }

            String xyd = ""+(seedx)+","+(seedy+1);
            System.out.println("Trying on xyd:"+xyd+",intensity:"+pixelValuesMap.get(xyd));
            if(pixelValuesMap.get(xyd)!= null && !xyl.equals(previousSeed)) {
                connectedValueTemp = calcConnectedValueTo(seedkey, xyd, pixelValuesMap, m1, m2, s1, s2);
                System.out.println("xyd:"+xyd+", connected value :"+ connectedValueTemp);
                if (connectedValueTemp < connectedValueMin) {
                    nextKey = xyd;
                } else {
                    pixelValuesMap.put(xyd, null);
                }
            }

            if(!nextKey.equals(xyl)){
                pixelValuesMap.put(xyl, null);
            }
            return nextKey;
        }

        public static float calcConnectedValueTo(String seedkey, String xy, Map pixelValuesMap, float m1, float m2, float s1, float s2) throws IOException, InterruptedException {

           int seedIntensity = (int) pixelValuesMap.get(seedkey);
            int pixelIntensity = (int) pixelValuesMap.get(xy);
            /*
            System.out.println("SeedIntensity = "+ seedIntensity+ ", pixelIntensity ="+pixelIntensity);
            System.out.printf("m1=%f,m2=%f,s1=%f,s2=%f\n",m1,m2,s1,s2);
            double g1 = Math.exp(Math.pow((((0.5*(seedIntensity + pixelIntensity))- m1)/s1),2)/2);
            double g2 = Math.exp(Math.pow((((0.5*(seedIntensity - pixelIntensity))- m2)/s2),2)/2);
            //float w1 = (float) (g1 / (g1+g2));
            //float w2 = 1 - w1;
            float w1 = 0.5f;
            float w2 = 0.5f;
            float mu = (float) (w1 * g1 + w2 * g2);
            return mu;
            */

            double term1 = 0.5*(seedIntensity + pixelIntensity);
            System.out.println("Term 1 ="+ term1);
            double term2 =  ((term1- m1)/(2*s1));
            System.out.println("Term 2 ="+ term2);
            double term3 = - Math.pow(term2,2);
            System.out.println("Term 3 ="+ term3);
            double g1 = Math.exp(term3);
            System.out.println("g1 ="+ g1);

            double termA = 0.5*(seedIntensity - pixelIntensity);
            System.out.println("Term A ="+ termA);
            double termB =  ((termA- m1)/(2*s1));
            System.out.println("Term B ="+ termB);
            double termC = - Math.pow(termB,2);
            System.out.println("Term C ="+ termC);
            double g2 = Math.exp(termC);
            System.out.println("g2 ="+ g2);

            //float w1 = (float) (g1 / (g1+g2));
            //float w2 = 1 - w1;
            float w1 = 0.5f;
            float w2 = 0.5f;
            float mu = (float) (w1 * g1 + w2 * g2);
            System.out.println("MU =" + mu);
            return mu;

        }


        private float[] getIntensityDeviationsofSumAndDiff(float[] meanResults, List<String> xyIntensityValues) {

            int size = xyIntensityValues.size();
            float m1 = meanResults[0];
            float m2 = meanResults[1];

            int deviationSum1 = 0;
            int deviationSum2 = 0;

            float[] sd = new float[2];

            for (String str : xyIntensityValues) {
                String[] pixelVal = str.split(",");
                int pixelIntensity = Integer.parseInt(pixelVal[2]);
                //System.out.println("pixel intensity-"+pixelIntensity);
                deviationSum1 = (int)(deviationSum1 +  Math.pow((pixelIntensity - m1),2) );
                deviationSum2 = (int)(deviationSum2 +  Math.pow((pixelIntensity - m2),2) );
            }
            sd[0] = (float) Math.sqrt(deviationSum1/size);
            sd[1] = (float) Math.sqrt(deviationSum2/size);
            return sd;
        }


        private static float[] getIntesityMeanOfSumAndDiff(List<String> xyIntensityValues){

            int size = 1;
            long preMeanSumofIntensitySum = 0;
            long preMeanSumofIntensityDiff = 0;
            float[] results = new float[2];
            for (String str: xyIntensityValues) {
                size ++;
                String[] pixelVal = str.split(",");
                int pixelIntensity = Integer.parseInt(pixelVal[2]);
                int seedIntensity = Integer.parseInt(pixelVal[3]);
                preMeanSumofIntensitySum += (0.5 *(seedIntensity + pixelIntensity));
                preMeanSumofIntensityDiff += (0.5 *(seedIntensity - pixelIntensity));
            }
            //System.out.println("Size of values in reduce - "+ size);
            results[0] = (preMeanSumofIntensitySum / size);
            results[1] =  (preMeanSumofIntensityDiff / size);
            return results;
        }

    }

    public static int[]convertToArray(List<String> xyIntensityList, int w, int h) {
        int[] pixelIntensityArray = new int[w * h];
        for (String val : xyIntensityList) {
            String[] pixelVal = val.split(",");
            int x = Integer.parseInt(pixelVal[0]);
            int y = Integer.parseInt(pixelVal[1]);
            int z = Integer.parseInt(pixelVal[2]);
            pixelIntensityArray[w*y+x] = z;
        }
        return pixelIntensityArray;
    }


    private static String[] parseSeedString(String seedStr){

        return seedStr.split(";");

    }

}
