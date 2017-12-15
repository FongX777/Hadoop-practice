package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.lang.Math;
import java.net.URI;
import java.util.Arrays;
import java.util.Comparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
/***************************************/
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class PageRank extends Configured implements Tool{
    //MUST CHECK
    //You Can Change the size of the Matrix dependent on your input file
    public static final int MATRIX_SIZE = 5;
    public static final String INFO = "Info";
    public static final String TONODE = "TONODE";
    public static final String TAB ="	";
    private static final String OUTPUTPATH_TMP = "/user/root/data/PageRank/intermediate_output";
    private static final String OUTPUTPATH_TMP2 = "/user/root/data/PageRank/intermediate_output2";
    private static final String OUTPUTPATH_TMP3 = "/user/root/data/PageRank/intermediate_output3";
    private static final String[] OUTPUTPATHS  = new String[50];
    //private static final String[] OUTPUTPATHS;
    public static final float BETA = 0.8f;
    public static final float EPSILON = 0.000001f;
    
    public static int NodeSize = 10876;
    //public static int NodeSize = 10876;
    public static boolean hasDeadEnds = false;


    
    public static class FirstMapper
        extends Mapper<LongWritable, Text, IntWritable, IntWritable>{

        private IntWritable OutputKey = new IntWritable();
        private IntWritable OutputValue = new IntWritable();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException{
            String inputLine  = value.toString();

            String[] elements = inputLine.split(TAB);

            //try{
                int fromNode = Integer.parseInt(elements[0]);
                int toNode = Integer.parseInt(elements[1]);

                OutputKey.set(fromNode);
                OutputValue.set(toNode);

                context.write(OutputKey, OutputValue);
           // }catch(NumberFormatException e){
            //}

        }
    }


    public static class FirstReducer
        extends Reducer<IntWritable, IntWritable, IntWritable, Text>{

        private IntWritable OutputKey = new IntWritable();
        private Text OutputValue = new Text();


        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException{

            //NodeSize ++;    //adding the NodeSize per key
            //pageRank, outdegree, j, k, ....
            StringBuilder colOfNode = new StringBuilder();
            int outDegree = 0;


            for (IntWritable value : values) {
                colOfNode.append(String.valueOf(value.get()));
                colOfNode.append(TAB);
                outDegree ++;
            }

            String initPageRank_str = String.valueOf((float)1/NodeSize);
            String outDegree_str =  String.valueOf(outDegree);
            colOfNode.insert(0, initPageRank_str + TAB + outDegree + TAB);

            OutputValue.set(new Text(colOfNode.toString()));

            
            context.write(key, OutputValue);


        }


    }

    //Matrix builder
    public static class SecondMapper
        extends Mapper<LongWritable, Text, IntWritable, Text>{

        private IntWritable OutputKey = new IntWritable();
        private Text OutputValue = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException{
            //value: fromNode, pageRank, outdegree, j, k, ...

            String inputLine  = value.toString();

            String[] elements = inputLine.split(TAB);

            String fromNode = "0";
            String pageRank = "0";
            String outDegree = "0";
            for(int i=0; i < elements.length; i++){
                if( i == 0)
                    fromNode = elements[0];
                else if( i == 1)
                    pageRank = elements[1];
                else if( i == 2){
                    outDegree = elements[2];
                }                    
                else{
                    int toNode = Integer.parseInt(elements[i]);

                    OutputKey.set(toNode);
                    OutputValue.set(fromNode + TAB + pageRank + TAB + outDegree);
                    context.write(OutputKey, OutputValue);
                }

            }
        }
    }

    
    public static class SecondReducer
        extends Reducer<IntWritable, Text, IntWritable, Text>{

        private IntWritable OutputKey = new IntWritable();
        private Text OutputValue = new Text();
        private StringBuilder outputSB = new StringBuilder();    //fromNode pageRank outdegree n1 n2 ...
        private static float S = 0.0f;



        public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException{
            //key: toNode, values:(fromNode pageRank outDegree)...


            int toNodeID = key.get();
            outputSB.setLength(0);

            int outDegreeCounter = 0;
            float pageRank = 0.0f;
            
            
            for (Text value : values) {

                Link tmpLink = new Link(toNodeID, value.toString());
                pageRank += (BETA * tmpLink.getPageRank()) / tmpLink.getOutDegree();
                
                outputSB.append(String.valueOf(tmpLink.getID()) + TAB );
                outDegreeCounter ++;
            }

            pageRank +=  (1-BETA) /NodeSize;

            outputSB.insert(0, String.valueOf(pageRank) + TAB
                               + String.valueOf(outDegreeCounter) + TAB);   //fromNode pageRank outdegree n1 n2 ...

            OutputValue.set(outputSB.toString());

            if(key.get() != -1)
                context.write(key, OutputValue);

            //UPDATE 
            S += pageRank;
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException{
            //context.write(new IntWritable(0), new Text(String.valueOf(S)));
            OutputKey.set(-1);
            OutputValue.set(String.valueOf((1.0f-S)) + TAB 
                            + "-1 -1");
            context.write(OutputKey, OutputValue);
        }


    }

    public static class Link{
        public Node from;
        public int toID;

        public Link(int toID, String str){
            //toID: toNode, str:(fromNode pageRank outDegree)
            String[] elements = str.split(TAB);
            this.toID = toID;
            this.from = new Node(Integer.parseInt(elements[0]),     //fromNode id
                            Float.parseFloat(elements[1]),          //pageRank
                            Integer.parseInt(elements[2]));         //outDegree
        }
        public int getID(){
                return this.from.id;
        }
        public float getPageRank(){
            return this.from.pageRank;
        }
        public int getOutDegree(){
            return this.from.outDegree;
        }

    }


    public static class Node{
        public int id;
        public float pageRank;
        public int outDegree;

        public Node(int id, float pageRank, int outDegree){
            this.id = id;
            this.pageRank = pageRank;
            this.outDegree = outDegree;
        }

        @Override
        public String toString(){
            return String.format("[Node] id: %d, rank: %d, outD: %d", 
                                  id, 
                                  pageRank,
                                  outDegree);
        }
    }

    //Matrix builder
    public static class SecondMapper_Last
        extends Mapper<LongWritable, Text, IntWritable, Text>{

        private IntWritable OutputKey = new IntWritable();
        private Text OutputValue = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException{
            //value: toNode, pageRank, outdegree, fromNodes(...)

            String inputLine  = value.toString();

            String[] elements = inputLine.split(TAB);    //split out toNode , pageRank , outDegree and others


            String toNode = elements[0];
            int toNodeID = Integer.parseInt(toNode);
            String pageRank = elements[1];
            String outDegree = elements[2];


            if(toNodeID == -1){
                //key: -1, value: S value
                OutputKey.set(toNodeID);
                OutputValue.set(elements[1]);
                context.write(OutputKey, OutputValue);
            }
            else {
                //key: toNode, value: toNode info
                OutputKey.set(toNodeID);
                OutputValue.set(INFO + TAB +pageRank + TAB + outDegree);
                context.write(OutputKey, OutputValue);

                //key: fromNode, value: toNode
                for(int i=3; i<elements.length; i++){
                    String fromNode = elements[i];

                    OutputKey.set(Integer.parseInt(fromNode));
                    OutputValue.set(TONODE + TAB + toNode);
                    context.write(OutputKey, OutputValue);
                }
            }




        }
    }

    //Matrix builder
    public static class ThirdMapper
        extends Mapper<LongWritable, Text, IntWritable, Text>{

        private IntWritable OutputKey = new IntWritable();
        private Text OutputValue = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException{
            //value: toNode, pageRank, outdegree, fromNodes(...)

            String inputLine  = value.toString();

            String[] elements = inputLine.split(TAB);    //split out toNode , pageRank , outDegree and others


            String toNode = elements[0];
            int toNodeID = Integer.parseInt(toNode);
            String pageRank = elements[1];
            String outDegree = elements[2];


            if(toNodeID == -1){
                //key: -1, value: S value
                OutputKey.set(toNodeID);
                OutputValue.set(elements[1]);
                context.write(OutputKey, OutputValue);
            }
            else {
                //key: toNode, value: toNode info
                OutputKey.set(toNodeID);
                OutputValue.set(INFO + TAB +pageRank + TAB + outDegree);
                context.write(OutputKey, OutputValue);

                //key: fromNode, value: toNode
                for(int i=3; i<elements.length; i++){
                    String fromNode = elements[i];

                    OutputKey.set(Integer.parseInt(fromNode));
                    OutputValue.set(TONODE + TAB + toNode);
                    context.write(OutputKey, OutputValue);
                }
            }




        }
    }

    
    public static class ThirdReducer
        extends Reducer<IntWritable, Text, IntWritable, Text>{

        private IntWritable OutputKey = new IntWritable();
        private Text OutputValue = new Text();
        private StringBuilder outputSB = new StringBuilder();    //fromNode pageRank outdegree n1 n2 ...
        private float S_val = 0.0f;
        private float addend = 0.0f;

        public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException{
            //key: fromNode, 
            //values:(info, outDegree)  (outNodes)+
        
            if(key.get() == -1){
                int count = 0;
                for(Text value: values){
                    String[] elements = value.toString().split(TAB);
                    S_val = Float.parseFloat(elements[0]);
                    addend = Float.parseFloat(elements[0])/NodeSize;

                    OutputKey = key;
                    OutputValue.set(elements[0]);

                    //context.write(OutputKey, OutputValue);

                    if(count == 0)
                        break;
                }
                return;
            }

            String pageRank = "0.0";
            String outDegree = "0";
            int outDegreeCounter = 0;
            for(Text value : values){
                String inputLine  = value.toString();
                String[] elements = inputLine.split(TAB);    //Only split out toNode and pageRank and others

                if(elements[0].equals(INFO)){
                   pageRank = String.valueOf(Float.parseFloat(elements[1]) 
                                             + addend);
                   outDegree = elements[2];
                }
                else if(elements[0].equals(TONODE)){
                   outputSB.append(elements[1] + TAB );
                   outDegreeCounter ++;
                }
            }
            outputSB.insert(0, pageRank + TAB 
                               + String.valueOf(outDegreeCounter) + TAB);

            OutputKey = key;
            OutputValue.set(outputSB.toString());
            context.write(OutputKey, OutputValue);

            outputSB.setLength(0);
        }

    }




    //Matrix builder
    public static class FouthMapper_Output
        extends Mapper<LongWritable, Text, IntWritable, Text>{

        private IntWritable OutputKey = new IntWritable();
        private Text OutputValue = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException{
            //value: fromNode, pageRank, outdegree, toNodes(...)

            String inputLine  = value.toString();

            String[] elements = inputLine.split(TAB);    //split out fromNode , pageRank , outDegree and others


            String toNode = elements[0];
            int toNodeID = Integer.parseInt(toNode);
            String pageRank = elements[1];
            float pageRank_val = Float.parseFloat(elements[1]);



            //key: toNode, value: toNode info
            OutputKey.set(toNodeID);
            OutputValue.set(pageRank );
            context.write(OutputKey, OutputValue);

        }
    }

    
    public static class FouthReducer_Output
        extends Reducer<IntWritable, Text, IntWritable, Text>{

        private IntWritable OutputKey = new IntWritable();
        private Text OutputValue = new Text();
        private NP[] nps = new NP[10];

        @Override
        public void setup(Context context) throws IOException, InterruptedException{
            for(int i=0; i<nps.length; i++){
                nps[i] = new NP(-1, -1);
            }

        }

        public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException{
            //key: Node, 
            //values: pageRank
        
            String pageRank = "";
            for(Text value : values){
                pageRank = value.toString();
            }
            int toNodeID = key.get();
            float pageRank_val = Float.parseFloat(pageRank);

            Arrays.sort(nps, new Comparator<NP>() {
                public int compare(NP np1, NP np2) {
                    return Float.compare(np2.pageRank, np1.pageRank);
                }
            });

            for(int i=nps.length-1; i>=0; i--){
                if(pageRank_val > nps[i].pageRank){
                    nps[i] = new NP(toNodeID, pageRank_val);
                    break;
                }
            }
            Arrays.sort(nps, new Comparator<NP>() {
                public int compare(NP np1, NP np2) {
                    return Float.compare(np2.pageRank, np1.pageRank);
                }
            });
        }
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException{
            Arrays.sort(nps, new Comparator<NP>() {
                public int compare(NP np1, NP np2) {
                    return Float.compare(np2.pageRank, np1.pageRank);
                }
            });
            for(int i=0; i<nps.length; i++){
                OutputKey.set(nps[i].NodeID);
                OutputValue.set(String.valueOf(nps[i].pageRank));
                context.write(OutputKey, OutputValue);
            }
        }

    }

    public static class NP{
        int NodeID;
        float pageRank;
        public NP(int NodeID, float pageRank){
            this.NodeID = NodeID;
            this.pageRank = pageRank;
        }
    }

     @Override
     public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);
        conf.set("mapred.job.tracker", "local");
        conf.set("mapreduce.output.textoutputformat.separator", TAB);

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        fs.delete(outputPath, true);


        for(int i=0; i<OUTPUTPATHS.length; i++){
            OUTPUTPATHS[i] =  OUTPUTPATH_TMP + String.valueOf(i);
            fs.delete(new Path(OUTPUTPATHS[i]), true);
        }
        int count = 0;


        /*
        * Job 1
        */
        Job job1 = Job.getInstance(conf, "Job1");
        job1.setJarByClass(PageRank.class);
        job1.setMapperClass(FirstMapper.class);
        job1.setReducerClass(FirstReducer.class);

        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);
        
        //job.setInputFormatClass(TextInputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job1, inputPath);
        FileOutputFormat.setOutputPath(job1, new Path(OUTPUTPATHS[count]));

        job1.waitForCompletion(true);

        
        int exitCode = 0;
        int totalTimes = 20;



        while(totalTimes -- > 0 ){
            /*
            * Job 2
            */
            hasDeadEnds = false;

            Job job2 = Job.getInstance(conf, "Job2");
            job2.setJarByClass(PageRank.class);
            job2.setMapperClass(SecondMapper.class);
            job2.setReducerClass(SecondReducer.class);

            job2.setMapOutputKeyClass(IntWritable.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(IntWritable.class);
            job2.setOutputValueClass(Text.class);
            

            FileInputFormat.addInputPath(job2, new Path(OUTPUTPATHS[count]));
            FileOutputFormat.setOutputPath(job2, new Path(OUTPUTPATHS[count+1]));
            job2.waitForCompletion(true);    //waiting for exit

            count++;

            Job job3 = Job.getInstance(conf, "Job3");
            job3.setJarByClass(PageRank.class);
            job3.setMapperClass(ThirdMapper.class);
            job3.setReducerClass(ThirdReducer.class);

            job3.setMapOutputKeyClass(IntWritable.class);
            job3.setMapOutputValueClass(Text.class);


            job3.setOutputKeyClass(IntWritable.class);
            job3.setOutputValueClass(Text.class);


            FileInputFormat.addInputPath(job3, new Path(OUTPUTPATHS[count]));

            FileOutputFormat.setOutputPath(job3, new Path(OUTPUTPATHS[count+1]));

            exitCode = job3.waitForCompletion(true) ? 0 : 1;

            count++;
                
            
        }        
                
        Job job4 = Job.getInstance(conf, "Job3");
        job4.setJarByClass(PageRank.class);
        job4.setMapperClass(FouthMapper_Output.class);
        job4.setReducerClass(FouthReducer_Output.class);

        job4.setMapOutputKeyClass(IntWritable.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setOutputKeyClass(IntWritable.class);
        job4.setOutputValueClass(Text.class);
         
        FileInputFormat.addInputPath(job4, new Path(OUTPUTPATHS[count]));
        FileOutputFormat.setOutputPath(job4, outputPath);


        /*for(int i=0; i<OUTPUTPATHS.length; i++){
            OUTPUTPATHS[i] =  OUTPUTPATH_TMP + String.valueOf(i);
            fs.delete(new Path(OUTPUTPATHS[i]), true);
        }*/

        return job4.waitForCompletion(true) ? 0 : 1;
     }

     /**
      * Method Name: main Return type: none Purpose:Read the arguments from
      * command line and run the Job till completion
      * 
      */
     public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Enter valid number of arguments <Inputdirectory>  <Outputlocation>");
            System.exit(0);
        }
        try{
        ToolRunner.run(new Configuration(), new PageRank(), args);
        }catch(Exception e){
            e.printStackTrace();
        }
    }


}

