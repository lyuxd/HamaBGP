package hama.rawdataprocess;
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;

public class RawDataMapper extends Mapper<LongWritable, Text, Text, Text>{

	@Override
	public void map(LongWritable Key,Text Value,Context context) throws IOException, InterruptedException{
		String line = Value.toString();
		String lineToSplit = line.substring(0, line.length()-2);
		//split
		String key1 = null;
		String value1 = null;
		String[] split = lineToSplit.split("> ");
		key1 = split[0]+">";
		value1 = split[1]+"> "+split[2]+" ";
		context.write(new Text(key1), new Text(value1));
		
		
		try {
			Node nodes[] = NxParser.parseNodes(line);
			String s = nodes[0].toString();
			String p = nodes[1].toString();
			//String o = (nodes[2] instanceof Literal ? nodes[2].toN3() : nodes[2].toString());
			if(!(split[2].toString().equals("\"\""))||(split[2].toString().equals("\"")))
			context.write(new Text(split[2]), new Text("<>"+s+"<>"+p));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
