package hama.rawdataprocess;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RawDataReducer extends Reducer<Text,Text,Text,Text>{
	public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
		String edges = "";
		String reverEdges = "";
		String tmp = null;
		for(Text value : values){
			tmp = value.toString();
			if(tmp.startsWith("<>"))
				reverEdges = reverEdges+tmp;
			else
				edges = edges+tmp;
		}
		edges = edges+".";
		context.write(key, new Text(edges+" "+reverEdges));
	}

}
