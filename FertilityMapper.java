package org.myorg;

//Importing the libraries that contain the classes and methods needed in the Mapper class
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//Creating a subclass from a parent class called Mapper using the keyword extends
//the input is key, value datatypes and output is key and DoubleWritable value
public class FertilityMapper  extends Mapper<LongWritable, Text, Text, DoubleWritable>
{
	//Fertility rate  is floating number which converted DoubleWritable for Hadoop processing
	//Country is string
	private final static DoubleWritable Fertility = new DoubleWritable(0);
	private Text Country = new Text();
	
	//@override is used to change the original behavior of the method “map” in the parent Class “Mapper”
	@Override
	// the map method takes three parameters
	public void map(LongWritable key, Text value, Context context)
	        throws IOException, InterruptedException
	{
		//split the values of each line on each comma
	    String[] line = value.toString().split(",");
	    //The country list is in the 1st column
	    String ID = line[0];
	    Country.set(ID);
	    //Fertility is in the 4th column
	    double fert = Double.parseDouble(line[3].trim());
	    Fertility.set(fert);
	    context.write(Country, Fertility);
	}
}
