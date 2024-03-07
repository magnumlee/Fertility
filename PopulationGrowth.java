package org.myorg;

//Importing the libraries that contain the classes and methods needed in the Mapper class
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;

public class PopulationGrowth 
{
	public static void main(String[] args) throws Exception
	{
		//create the “conf” object from the “Configuration” class, which provides access to 
		//configuration parameters necessary for Hadoop job
	    Configuration conf = new Configuration();
	    if (args.length != 3)
	    {
	        System.err.println("Usage: MeanPopulationGrowth <input path> <output path>");
	        System.exit(-1);
	    }
	    
	    Job job;
	    //configure parameters for the created job
	    job=Job.getInstance(conf, "Mean PopulationGrowth");
	    //specifying the driver class in the Jar file
	    job.setJarByClass(PopulationGrowth.class);
	    //setting the input and output paths for the job
	    FileInputFormat.addInputPath(job, new Path(args[1]));
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
	    //set the Mapper and Reducer
	    job.setMapperClass(PopulationGrowthMapper.class);
	    job.setReducerClass(PopulationGrowthReducer.class);
	    //Setting the data types for the key and value pair that will be used to write output data.
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    
	    // Delete output if exists
	    FileSystem hdfs = FileSystem.get(conf);
	    Path outputDir = new Path(args[2]);
	    if (hdfs.exists(outputDir))
	        hdfs.delete(outputDir, true);
	    //exits the system after completion
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
