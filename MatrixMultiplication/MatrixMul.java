package org.apache.hadoop.examples;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.lang.Math;
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

public class MatrixMul {
    //You Can Change the size of the Matrix dependent on your input file
    public static final int MATRIX_SIZE = 500;

    public static class TokenizerMapper
        extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

        public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one);
            }
        }
    }

    public static class MatrixMulMapper
        extends Mapper<LongWritable, Text, Text, Text>{
        
        private Text MapOutputKey = new Text();
        private Text MapOutputValue = new Text();
        private String[] elements;
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException{
            
            String inputLine  = value.toString();

            String[] elements = inputLine.split(",");

                if(elements[0].equals("M")){
                    //key-pair: ((i,k) : (M, j, Mij))
                    for (int k=0; k<MATRIX_SIZE; k++){
                        
                        MapOutputKey.set(getFormatedNumString(elements[1]) + "," + 
                                         getFormatedNumString(k));   //i, k
                        MapOutputValue.set(elements[0] + "," + elements[2] + 
                                        "," + elements[3]);     //M, j, Mij
                        context.write(MapOutputKey, MapOutputValue);
                    }
                }
                else if(elements[0].equals("N")){
                    //key-pair: ((i,k) : (N, i, Nij))
                    for (int i=0; i<MATRIX_SIZE; i++){
                        MapOutputKey.set(getFormatedNumString(i) + "," + 
                                         getFormatedNumString(elements[2]));   //i, k 
                        MapOutputValue.set(elements[0] + "," + elements[1] + 
                                        "," + elements[3]);     //N, j, Njk
                        context.write(MapOutputKey, MapOutputValue);
                    }
                }
        }
        //ex: 0 -> 00000, 1 -> 00001
        public static String getFormatedNumString(String num_str){
            int number = Integer.parseInt(num_str);
            
            int maxLength = 4;
            for(int i=1; i<=maxLength; i++){

                if(number < Math.pow(10, i)){
                    String prefixZero = "";
                    for(int j=0; j<maxLength-i; i++)
                        prefixZero += "0";

                    return prefixZero + num_str;
                }
            }
            return num_str;
        }
        public static String getFormatedNumString(int num){
            return getFormatedNumString(Integer.toString(num));
        }

    }


    public static class MatrixMulCombiner
        extends Reducer<Text, Text, Text, Text>{
        
        private Text ReduceOutputKey = new Text();
        private Text ReduceOutputValue = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException{
            String[] elements;
            //key, Values
            
            HashMap<Integer, Integer>hashM = new HashMap<Integer, Integer>();
            HashMap<Integer, Integer>hashN = new HashMap<Integer, Integer>();
            for(Text value: values){
                elements = value.toString().split(",");
                if(elements[0].equals("M")){
                    hashM.put(Integer.parseInt(elements[1]), Integer.parseInt(elements[2]));    //(M, j, Mij))
                    
                }else if (elements[0].equals("N")){
                    hashN.put(Integer.parseInt(elements[1]), Integer.parseInt(elements[2]));    //(N, j, Njk))

                }
            }

            int result = 0;
            int Mij;
            int Njk;
            for (int j=0; j<MATRIX_SIZE; j++){
                Mij = hashM.containsKey(j) ? hashM.get(j) : 0;
                Njk = hashN.containsKey(j) ? hashN.get(j) : 0;
                result += Mij * Njk;
            }


            ReduceOutputKey = key;
            ReduceOutputValue.set(Integer.toString(result));
            context.write(ReduceOutputKey, 
                            ReduceOutputValue);
        }
    }


    public static class MatrixMulReducer
        extends Reducer<Text, Text, Text, IntWritable>{
        
        private Text ReduceOutputKey = new Text();
        private IntWritable ReduceOutputValue = new IntWritable();
        public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException{
            String[] elements;
            //key, Values
            
            HashMap<Integer, Integer>hashM = new HashMap<Integer, Integer>();
            HashMap<Integer, Integer>hashN = new HashMap<Integer, Integer>();
            for(Text value: values){
                elements = value.toString().split(",");
                if(elements[0].equals("M")){
                    hashM.put(Integer.parseInt(elements[1]), Integer.parseInt(elements[2]));    //(M, j, Mij))
                    
                }else if (elements[0].equals("N")){
                    hashN.put(Integer.parseInt(elements[1]), Integer.parseInt(elements[2]));    //(N, j, Njk))

                }
            }

            int result = 0;
            int Mij;
            int Njk;
            for (int j=0; j<MATRIX_SIZE; j++){
                Mij = hashM.containsKey(j) ? hashM.get(j) : 0;
                Njk = hashN.containsKey(j) ? hashN.get(j) : 0;
                result += Mij * Njk;
            }

            String keys[] = key.toString().split(",");
            String newKey = Integer.toString(Integer.parseInt(keys[0])) + ","
                            + Integer.toString(Integer.parseInt(keys[1]));

            ReduceOutputKey.set(newKey);
            ReduceOutputValue.set(result);

                            
            context.write(ReduceOutputKey, 
                            ReduceOutputValue);
        }
    }



public static class IntSumReducer
        extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                        Context context
                        ) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
        sum += val.get();
        }
       result.set(sum);
       context.write(key, result);
    }
}

public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
        System.err.println("Usage: wordcount <in> <out>");
        System.exit(2);
    }
    System.out.println("=========START HAHA=========");
    conf.set("mapred.job.tracker", "local");
    conf.set("mapreduce.output.textoutputformat.separator", ",");



    Job job = new Job(conf, "word count");
    job.setJarByClass(MatrixMul.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(MatrixMulMapper.class);
    job.setReducerClass(MatrixMulReducer.class);




    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

