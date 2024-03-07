package org.myorg;
//Importing the libraries that contain the classes and methods needed in the Reducer class
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FertilityReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
{
	ArrayList<Double> FertilityList = new ArrayList<Double>();
	int rateSize = FertilityList.size();
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
	        throws IOException, InterruptedException
	{
		//create a integer and initialised as 0
		int sum = 0;
		//create a double variable and initialised as 0
		double SumofFertility = 0.0;
		double minValue = 0.0;
		double maxValue = 0.0;
		rateSize = FertilityList.size();
	    for (DoubleWritable value : values) 
	    {
	    	sum += value.get();
	    	//add values to ArrayList
	    	FertilityList.add(value.get());
	    	//sum of all values is calculated
	    	SumofFertility = SumofFertility + value.get();
	    	//minimum value is selected
	    	minValue = Math.min(minValue, value.get());
	    	//maximum value is selected
	    	maxValue = Math.max(maxValue, value.get());
	    }
	    //get the number of fertility with ArrayList in ascending orders
	    Collections.sort(FertilityList);
	    int size = FertilityList.size() - rateSize;
	    //The mean is equalled to the sum of values divided by the size of values
	    double mean = SumofFertility / size;
	    //outputs total values
	    context.write(key, new DoubleWritable(sum));
	    //outputs the minimum value
	    context.write(new Text("Minimum Value:     "), new DoubleWritable(minValue));
	    //outputs the maximum value
	    context.write(new Text("Maximum Value:     "), new DoubleWritable(maxValue));
	    //outputs the mean value
	    context.write(new Text("Mean:              "), new DoubleWritable(mean));
	}
}
