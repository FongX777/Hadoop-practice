package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.lang.Math;
import java.net.URI;
import java.util.Arrays;
import java.util.Comparator;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.GenericOptionsParser;
/***************************************/
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class Kmeans extends Configured implements Tool{
    //MUST CHECK
    //You Can Change the size of the Matrix dependent on your input file
    public static final String SPACE = " ";

    private static final String OUTPUTPATH_TMP = "/user/root/data/kmeans/intermediate_output";
    private static final String DATA_INPUTPATH = "/user/root/data/kmeans/data.txt";
    private static final String C_INPUTPATH = "/user/root/data/kmeans/c1.txt";
    //private static final String C_INPUTPATH = "/user/root/data/kmeans/c2.txt";
    private static final String C_OUTPUTPATH = "/user/root/output/costs.csv";


    public static final int MAX_ITER = 20;
    public static final int MAX_CEN = 10;
    public static final int MAX_DIM = 58;
    public static final int NODE_COUNT = 4601;

    
    public static class Map
        extends Mapper<LongWritable, Text, IntWritable, Text>{

        private IntWritable OutputKey = new IntWritable();
        private Text OutputValue = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException{

                Configuration conf = context.getConfiguration();
                String[] dims = value.toString().split(SPACE);
                double[][] centroids = new double [MAX_CEN][MAX_DIM];
                double[] point = new double[MAX_DIM];


                for(int i=0;i<MAX_CEN;i++){
                    for(int j=0;j<MAX_DIM;j++){
                        double cDoubleIJ = conf.getDouble("C_"+String.valueOf(i)+"_"+String.valueOf(j), 0.0);
                        centroids[i][j] = cDoubleIJ;
                    }
                }

                for(int i=0;i<MAX_DIM;i++){
                    point[i] = Double.parseDouble(dims[i]);
                }

                double[] distancesToCen = new double[MAX_CEN];
                double minDistance = 1e9;
                int minIndex = 0;
                for(int i=0; i<MAX_CEN; i++){
                    distancesToCen[i] = 0;
                    for(int j=0; j<MAX_DIM; j++){
                       double subtraction = centroids[i][j] - point[j];
                       //distancesToCen[i] += Math.pow(subtraction, 2);   //Euclidean 
                       distancesToCen[i] += Math.abs(subtraction);         //Manhattan
                    }

                    //distancesToCen[i] = Math.sqrt(distancesToCen[i]);   //Euclidean

                    if(distancesToCen[i] < minDistance){
                        minDistance = distancesToCen[i];
                        minIndex = i;
                    }
                }

                OutputKey.set(minIndex);
                OutputValue.set(value.toString() + " " + Double.toString(minDistance));
                context.write(OutputKey, OutputValue);
        }
    }


    public static class Reduce
        extends Reducer<IntWritable, Text, IntWritable, Text>{

        private IntWritable OutputKey = new IntWritable();
        private Text OutputValue = new Text();

        MultipleOutputs<IntWritable, Text> mos;

        public void setup(Context context){
            mos = new MultipleOutputs(context);
        }
        

        public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException{
                
                double[] newCentroid = new double[MAX_DIM];
                double cost = 0;
                int clusterNodeCount = 0;
                String outputLine = "";

                for(Text val : values){
                    String[] elements = val.toString().split(SPACE);
                    for(int i=0; i<MAX_DIM; i++)
                        newCentroid[i] += Double.parseDouble(elements[i]);
                    
                    cost += Double.parseDouble(elements[MAX_DIM]);
                    clusterNodeCount++;
                }

                for(int i=0; i<MAX_DIM; i++){
                    newCentroid[i] = newCentroid[i] / (double)clusterNodeCount;
                    outputLine += String.valueOf(newCentroid[i]) + SPACE;
                }

                mos.write("centroid", NullWritable.get(), new Text(outputLine));
                mos.write("cost", NullWritable.get(), new Text(String.valueOf(cost)));

        }

        protected void cleanup(Context context) throws IOException, InterruptedException{
            mos.close();
        }

    }


     @Override
     public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);
        conf.set("mapred.job.tracker", "local");
        conf.set("mapreduce.output.textoutputformat.separator", SPACE);

        Path inputPath_data = new Path(args[0]);       //data.txt
        Path inputPath_cen = new Path(args[1]);       //cx.txt    x=1,2
        
        Path outputPath = new Path(args[2]);        //output
        fs.delete(outputPath, true);
        fs.delete(new Path(C_OUTPUTPATH), true);


        fs.delete(new Path("/user/root/output/tmp/"), true);

        int count = 0;
        int returnValue = 0;
        Double[][] centroids = new Double[MAX_CEN][MAX_DIM];
        String cen_file = "";

        while(count++ < MAX_ITER){

            if(count == 1)
                //cen_file = C_INPUTPATH;
                cen_file = args[1];
            else 
                cen_file = "/user/root/output/tmp/centroid-r-00000";




            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(cen_file))));
            String inputLine = br.readLine();
            int lineCount = 0;
            while(inputLine != null){
                String[] elements = inputLine.split(SPACE);
                for(int i=0; i<MAX_DIM; i++){
                    centroids[lineCount][i] = Double.parseDouble(elements[i]);
                }
                inputLine = br.readLine();

                lineCount ++;
                }

            if(count != 1){
                fs.delete(new Path("/user/root/output/tmp/"), true);
            }


            // run /*
            for(int i=0; i<MAX_CEN;i++)
                for(int j=0; j<MAX_DIM; j++)
                    conf.setDouble("C_" + String.valueOf(i) + "_"+String.valueOf(j), centroids[i][j]);
            
            conf.set("centroid", "centroid");

            Job job = Job.getInstance(conf, "job");
            job.setJarByClass(Kmeans.class);
            job.setMapperClass(Map.class);
            job.setReducerClass(Reduce.class);

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);


            MultipleInputs.addInputPath(job, inputPath_data, TextInputFormat.class, Map.class);

            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job, new Path("/user/root/output/tmp"));

            MultipleOutputs.addNamedOutput(job, "centroid", TextOutputFormat.class, NullWritable.class, Text.class);
            MultipleOutputs.addNamedOutput(job, "cost", TextOutputFormat.class, NullWritable.class, Text.class);

            returnValue = job.waitForCompletion(true)? 0 : 1;
            //run */
            //
            calCostAndWriteFile(count);
        }

        return returnValue;
     }

     public static void calCostAndWriteFile(int n){
        
         BufferedReader br = null;
         BufferedWriter bw = null;

         try{
             Path path = new Path("/user/root/output/tmp/cost-r-00000");
             FileSystem fs = FileSystem.get(new Configuration() );
             br = new BufferedReader( new InputStreamReader(fs.open(path)) );
             String line = br.readLine();
             double cost = 0.0;

             while(line != null)
             {
                 cost += Double.parseDouble(line);
                 line = br.readLine();
             }

             if( !fs.exists(new Path(C_OUTPUTPATH)))
                 fs.createNewFile(new Path(C_OUTPUTPATH));

             bw = new BufferedWriter( new OutputStreamWriter(fs.append(new Path(C_OUTPUTPATH)), "UTF-8"));
             //bw = new BufferedWriter(fw);
             bw.write(String.valueOf(n) + "," + String.valueOf(cost) + "\n");

             br.close();
             bw.close();
         }catch(IOException ex){
            ex.printStackTrace();
         }finally{
         }

     }

     /**
      * Method Name: main Return type: none Purpose:Read the arguments from
      * command line and run the Job till completion
      * 
      */
     public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Enter valid number of arguments <Inputdirectory>  <Outputlocation>");
            System.exit(0);
        }
        try{
            ToolRunner.run(new Configuration(), new Kmeans(), args);
        }catch(Exception e){
            e.printStackTrace();
        }
    }


}

