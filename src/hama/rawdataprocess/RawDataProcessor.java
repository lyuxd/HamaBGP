package hama.rawdataprocess;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
//import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class RawDataProcessor {
	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		if (args.length < 2) {
			System.err.println("erro usage.");
			System.exit(1);
		}
		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator"," ");
		Job job = new Job(conf,"RawDataProcess");
		job.setJarByClass(RawDataProcessor.class);

		job.setMapperClass(RawDataMapper.class);
		job.setReducerClass(RawDataReducer.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

	}
}
