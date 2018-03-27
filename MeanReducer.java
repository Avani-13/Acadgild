import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MeanReducer
extends Reducer<Text, IntWritable, Text, FloatWritable> {

	private FloatWritable result=new FloatWritable();
	Float mean = 0f;
	Float count = 0f;
	int sum = 0;
@Override
public void reduce(Text key, Iterable<IntWritable> values,
    Context context)
    throws IOException, InterruptedException {
    /*Text sumText = new Text("mean");	*/
    for (IntWritable value : values) {
		sum+=value.get();	
     }
    count+=1;
    mean = sum/count;
    result.set(mean);
     context.write(key, result);
}
}
