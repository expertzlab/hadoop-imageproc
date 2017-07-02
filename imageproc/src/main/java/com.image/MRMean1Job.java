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

            String seedString = "229,122,29";
            //"241,92,25;"+
            //"249,65,15;"+
            //"285,43,44;"+
            //"324,67,29;"+
            //"314,113,24;";
            String[] seedPointsArray = seedString.split(";");

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
            //int seedx = Integer.parseInt(seedPoint[0]);
            //int seedy = Integer.parseInt(seedPoint[1]);

            List<String> xyCoordinates = new ArrayList();
            int w = 1;
            int h = 1;
            int seedIntensity = -1;
            String[] xyzstrings = null;
            for (Text val : values) {
                xyzstrings = val.toString().split(";");

                for(String coordinates: xyzstrings) {

                    String[] str = coordinates.split(",");
                        int x = Integer.parseInt(str[0]);
                        int y = Integer.parseInt(str[1]);
                        if(seedIntensity == -1) {
                            seedIntensity = Integer.parseInt(str[3]);
                        }
                        if (x > w) {
                            w = x;
                        }
                        if (y > h) {
                            h = y;
                        }
                        //System.out.printf("image width-%d,height-%d\n",w,h);
                        xyCoordinates.add(coordinates);
                }
            }
            System.out.println("BEGIN : Size of values in reduce : "+ xyCoordinates.size());
            System.out.println("seed key:"+key);

            reduceOnConnectedValue(key, xyCoordinates, context, seedPoint, w, h);

        }

        public static void reduceOnConnectedValue(Text seedKey, List<String> xyIntensityList, Reducer<Text, Text, Text, Text>.Context context, String[] seedPoint, int w, int h) throws IOException, InterruptedException {


            short[] pixelIntensityArray = convertToArray(xyIntensityList, w, h);


            int seedx = Integer.parseInt( seedPoint[0]);
            int seedy = Integer.parseInt( seedPoint[1]);

            int[] seeds = new int[1];
            seeds[0] = seedx + seedy * w;

            printScenePoints(pixelIntensityArray, seeds,w,  h, context);

            /*
            for(int y=0; y < h; y++){
                    for(int x=0; x < w; x++){
                    String xy = ""+x+","+y+",";
                    int position = x+(y==0?0:y-1)*h;
                    if(connectedScene[position]> 0.6) {
                        context.write(new Text(xy), new Text("" + pixelIntensityArray[position]));
                    }
                }
            }
            */

        }

        private static float[] printScenePoints(short[] m_imagePixels, int[] m_seeds, int w, int h, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

            DialCache m_dial = new DialCache();
            float m_threshold = 0.6f;
            int n = w * h;
            float[] m_conScene = new float[n];
            m_conScene[m_seeds[0]] = 1.0f;
            m_dial.Push(m_seeds[0], DialCache.MaxIndex);

            System.out.println("First Seed at -"+ (m_seeds[0]%w)+","+(m_seeds[0]/w));
            while(m_dial.m_size > 0)
            {
                int c = m_dial.Pop();
                context.write(new Text((c%w)+","+(c/w)), new Text("," + m_imagePixels[c]));
                float[] meanSigmaResults = new float[4];
                calculateMeansAndSigmas(c, meanSigmaResults, m_imagePixels, w, h);

                int[] neighbors = getNeighbors(c,w ,h);
                for(int e : neighbors)
                {

                    //We get -1 when we are at an edge (e.g. on first row and want the neighbor on the row below)
                    if(e == -1)
                        continue;

                    System.out.println("selected neighbouring points at -"+ (e%w)+","+(e/w));
                    float aff_c_e = affinity(c, e, meanSigmaResults, m_imagePixels);
                    System.out.println("Connectedness value:"+aff_c_e);

                    if(aff_c_e < m_threshold)
                        continue;

                    float f_min = Math.min(m_conScene[c], aff_c_e);
                    if(f_min > m_conScene[e])
                    {
                        m_conScene[e] = f_min;

                        if(m_dial.Contains(e))
                            m_dial.Update(e, (int)(DialCache.MaxIndex * f_min + 0.5f));
                        else
                            m_dial.Push(e, (int)(DialCache.MaxIndex));
                        System.out.println("connectedness value: "+f_min);
                        System.out.println("pushed -"+ (e%w)+","+(e/w));

                    }
                }
            }

            return m_conScene;

        }

        private static void calculateMeansAndSigmas(int c, float[] meanSigmaResults, short[] m_imagePixels, int w, int h)
        {

            //Will never add duplicates, since we use HashSets
            HashSet<Integer> spels = new HashSet<Integer>();

                int[] neighbors = getNeighbors(c, w, h);
                for(int j : neighbors)
                {
                    if(j == -1)
                        continue;

                    int[] neighborsNeighbors = getNeighbors(j, w, h);

                    for(int k : neighborsNeighbors)
                    {
                        if(k == -1)
                            continue;

                        spels.add(k);
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
                    aves[count] = ave(spelsArray[i], spelsArray[j], m_imagePixels);
                    reldiffs[count] = reldiff(spelsArray[i], spelsArray[j],m_imagePixels);

                    count++;
                }
            }

            float[] ave_meanSigma = welford(aves);
            float[] reldiff_meanSigma = welford(reldiffs);

            //m_mean_ave = ave_meanSigma[0];
            meanSigmaResults[0] = ave_meanSigma[0];
            //m_sigma_ave = ave_meanSigma[1];
            meanSigmaResults[1] = ave_meanSigma[1];

            //m_mean_reldiff = reldiff_meanSigma[0];
            meanSigmaResults[2] = reldiff_meanSigma[0];
            //m_sigma_reldiff = reldiff_meanSigma[1];
            meanSigmaResults[3] = reldiff_meanSigma[1];

            System.out.println("ave mean: " + meanSigmaResults[0]);
            System.out.println("ave sigma: " + meanSigmaResults[1]);
            System.out.println("reldiff mean: " + meanSigmaResults[2]);
            System.out.println("reldiff sigma: " + meanSigmaResults[3]);
        }

        /**
         * Single-pass average and standard deviation calculation
         * https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm
         * @param els list of elements to calculate the average and standard deviation of
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

        private static float affinity(int c, int d, float[] meanSigmaResults, short[] m_imagePixels)
        {
            float g_ave = gaussian(ave(c, d, m_imagePixels), meanSigmaResults[0], meanSigmaResults[1]);
            float g_reldiff = gaussian(reldiff(c, d, m_imagePixels), meanSigmaResults[2], meanSigmaResults[3]);

            //System.out.println("ave mean: " + meanSigmaResults[0]);
            //System.out.println("ave sigma: " + meanSigmaResults[1]);
            //System.out.println("reldiff mean: " + meanSigmaResults[2]);
            //System.out.println("reldiff sigma: " + meanSigmaResults[3]);

            return Math.min(g_ave, g_reldiff);
        }

        private static float ave(int c, int d, short[] m_imagePixels)
        {
            return 0.5f * ((float)m_imagePixels[c] + (float)m_imagePixels[d]);
        }

        private static float reldiff(int c, int d, short[] m_imagePixels)
        {
            float fc = m_imagePixels[c];
            float fd = m_imagePixels[d];

            return fc == -fd ? 0 : (Math.abs(fc - fd)) / (fc + fd);
        }

        private static int[] getNeighbors(int c, int w, int h)
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


        public static short[]convertToArray(List<String> xyIntensityList, int w, int h) {

            short[] pixelIntensityArray = new short[w * h+100];
            for (String val : xyIntensityList) {
                String[] pixelVal = val.split(",");
                short x = Short.parseShort(pixelVal[0]);
                short y = Short.parseShort(pixelVal[1]);
                short z = Short.parseShort(pixelVal[2]);
                //System.out.printf("x=%d,y=%d,z=%d\n",x,y,z);
                pixelIntensityArray[w*(y==0?0:y-1)+x] = z;
            }
            return pixelIntensityArray;
        }

    }


}
