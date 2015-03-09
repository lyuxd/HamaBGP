package hama.bgp;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.Vertex;
import org.apache.hama.graph.VertexInputReader;
import org.apache.log4j.Logger;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;

public class HamaBgpVertexReader extends
			VertexInputReader<LongWritable, Text, Text, Text, Text> {

		private static final Logger log = Logger
				.getLogger(HamaBgpVertexReader.class);

		/**
		 * The text file essentially should look like: <br/>
		 * 
		 * etc.
		 */
		@Override
		public boolean parseVertex(LongWritable key, Text value,
				Vertex<Text, Text, Text> vertex) {
//			String line = value.toString();
//
//			Node[] nodes = null;
//			try {
//				nodes = NxParser.parseNodes(line);
//			} catch (ParseException e) {
//				// if there is a syntax error, we just log and ignore it.
//				log.debug(e.getMessage());
//				return false;
//			}
//			assert (nodes != null);
//
//			String s = nodes[0].toString();
//			String p = nodes[1].toString();
//			String o = (nodes[2] instanceof Literal ? nodes[2].toN3()
//					: nodes[2].toString());
//
//			vertex.setVertexID(new Text(s));
//			vertex.setValue(new Text(""));
//			vertex.addEdge(new Edge<Text, Text>(new Text(o), new Text(p)));
//
//			return true;
			
			//log.info("Creating Graph...");
			String line = value.toString();
			String[] split = line.split(" <>");
			Node[] nodes = null;
			try {
				//System.out.println(split.length);
				nodes = NxParser.parseNodes(split[0]);
			} catch (ParseException e) {
				// if there is a syntax error, we just log and ignore it.
				log.debug(e.getMessage());
				return false;
			}
			assert (nodes != null);

			String s = (nodes[0] instanceof Literal ? nodes[0].toN3() : nodes[0].toString());
			vertex.setVertexID(new Text(s));
			
			if(split.length == 2)
				vertex.setValue(new Text(split[1]));
			else
				vertex.setValue(new Text(""));
			
			for(int i=1;i+1<nodes.length;i+=2){
				vertex.addEdge(new Edge<Text, Text>(new Text(nodes[i+1] instanceof Literal ? nodes[i+1].toN3()
						: nodes[i+1].toString()),new Text(nodes[i].toString())));
			}
			return true;
		}

	}