package org.myorg;
//Importing the libraries that contain the classes and methods needed in the Reducer class
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PopulationGrowthReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
{
	ArrayList<Double> PopulationGrowthList = new ArrayList<Double>();
	int growthSize = PopulationGrowthList.size();
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
	        throws IOException, InterruptedException
	{
		//create a integer and initialised as 0
	    int sum = 0;
	    //create a double variable called SumofPopulationGrowth and initialised as 0
		double SumofPopulationGrowth = 0.0;
		double minValue = 0.0;
		double maxValue = 0.0;
		growthSize = PopulationGrowthList.size();
	    for (DoubleWritable value : values) 
	    {
	    	sum += value.get();
	    	//add values to ArrayList
	    	PopulationGrowthList.add(value.get());
	    	//sum of all values is calculated
	    	SumofPopulationGrowth = SumofPopulationGrowth + value.get();
	    	//minimum value is selected
	    	minValue = Math.min(minValue, value.get());
	    	//maximum value is selected
	    	maxValue = Math.max(maxValue, value.get());
	    }
	    //get the number of fertility with ArrayList in ascending orders
	    Collections.sort(PopulationGrowthList);
	    int size = PopulationGrowthList.size() - growthSize;
	    //The mean is equalled to the sum of values divided by the size of values
	    double mean = SumofPopulationGrowth / size;
	    //outputs total values
	    context.write(key, new DoubleWritable(sum));
	    context.write(new Text("Minimum Value:     "), new DoubleWritable(minValue));
	    //outputs the maximum value
	    context.write(new Text("Maximum Value:     "), new DoubleWritable(maxValue));
	    //outputs the mean value
	    context.write(new Text("Mean:              "), new DoubleWritable(mean));
	}
}
