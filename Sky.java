package edu.cs236.skyline;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

  public class Sky {

    private static long key = 0;
    private static int Partition;

    public static synchronized int getPartition() {
        return Partition;
    }    
	public static void main(String[] args) throws IOException {
        Path input = null, output = null;
        int reducers;
        int x = 0;
        Partition = 1000;
        Configuration conf = new Configuration();
        reducers = Integer.parseInt(args[0]);
        input = new Path(args[1]);
        while (getPartition() >= 1) {
        	
        	conf.set("Partition", Integer.toString(Partition));
            Job job = new Job(conf, "skyline");
            job.setJarByClass(Sky.class);
            output = new Path(args[2] + x);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Gsod.class);
            job.setMapperClass(Map.class);
            

            if ((getPartition() / 10) >= 1) {
                job.setReducerClass(Reduce.class);
            } else {
                job.setReducerClass(LastReduce.class);
            }

            job.setNumReduceTasks(reducers);

            FileInputFormat.addInputPath(job, input);
            FileOutputFormat.setOutputPath(job, output);

            try {
                job.waitForCompletion(true);
            } catch (InterruptedException e) {
                System.out.println("Interrupted Exception");
            } catch (ClassNotFoundException e) {
                System.out.println("ClassNotFoundException");
            }

            try {
                input = new Path(args[2] + x);
            } catch (ArrayIndexOutOfBoundsException e) {
                e.printStackTrace();
            }
            setPartition(10);
            x++;
        }
	public static synchronized void setPartition(int m) {
        Partition = Partition / m;
    }
	public static synchronized long getKey() {
        return key++;
    }
    }
}
