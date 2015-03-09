/*
 * this class can only process the simplest query.
 * such as ?s p o and s p ?o
 */
package hama.bgp;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.TextInputFormat;

import org.apache.hama.graph.Edge;
import org.apache.hama.graph.GraphJob;
import org.apache.hama.graph.Vertex;
import org.apache.hama.graph.VertexInputReader;

import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;

import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;

public class HamaBgp {

	public static class HamaBgpVertex extends Vertex<Text, Text, Text> {
		private static final Logger log = Logger.getLogger(HamaBgp.class);

		int numEdges;
		String query;

		@Override
		public void setup(Configuration conf) {
			query = conf.get("query");
			numEdges = this.getEdges().size();

		}

		@Override
		public void compute(Iterator<Text> messages) throws IOException {
			if (!this.getVertexID().toString().equals("MasterNode")) {
				if (this.getSuperstepCount() == 0) {// the first superstep,send
													// message to son.
					List<Edge<Text, Text>> edges = this.getEdges();

					for (Edge<Text, Text> edge : edges) {

						this.sendMessage(edge, new Text(this.getVertexID()
								+ "<>" + edge.getValue()));
					}
					//voteToHalt();
				} else if (this.getSuperstepCount() == 1) {// the second
															// superstep ,
															// update vertex's
															// value
					/*
					 * 第一个superstep，接收上个superstep传递过来的边信息。
					 * */
					while (messages.hasNext()) {
						String msg = messages.next().toString();
						String value = this.getValue().toString();
						if (value == null || value.equals("")) {
							value = msg;
						} else {
							value = value + "<>" + msg;
						}
						this.setValue(new Text(value));
					}
					// select one triple from the total query sentence.
					String subquery = SelectOneTripleFromQuery(query);
					Node nodes[] = null;
					try {
						nodes = NxParser.parseNodes(subquery);
					} catch (ParseException e) {

						// if there is a syntax error, we just log and ignore
						// it.
						log.debug(e.getMessage());
					}

					String s = nodes[0].toString();
					String p = nodes[1].toString();
					String o = (nodes[2] instanceof Literal ? nodes[2].toN3()
							: nodes[2].toString());

					if (isKnown(s) && !isKnown(o)) {// s p ?o
						
						if (this.getVertexID().toString().equals(s)) {
							List<Edge<Text, Text>> edges = this.getEdges();
							
							for (Edge<Text, Text> edge : edges) {
								if (edge.getValue().toString().equals(p)) {
									// send s p edge.getdestinationid to
									// masternode.
									
									String var = o.substring(o.indexOf('?'),
											o.length());
									String queryCopy = query.replaceAll("<[?]"
											+ var + ">",
											"<" + edge.getDestinationVertexID()
													+ ">");
									sendMessage(new Text("MasterNode"),
											new Text(queryCopy));
								}
							}
						} //else
							//voteToHalt();
					} else if ((!isKnown(s)) && isKnown(o)) {// ?s p o
						// treat o as a vertex , but in fact not all o is
						// vertex,(tom age 35) is of this case.

						// if(this.getVertexID().toString().equals(o)){// It is
						// me!!!
						//
						// String[] revertedge =
						// this.getValue().toString().split("<>");
						//
						// for(int i=0;2*i<revertedge.length;i++){
						// if(revertedge[2*i+1].equals(p)){
						// String var = s.substring(s.indexOf('?'),s.length());
						// String queryCopy = query.replaceAll("<[?]"+var+">",
						// "<"+revertedge[2*i]+">");
						//
						// sendMessage(new Text("MasterNode"), new
						// Text(queryCopy));
						// }
						// }
						// }

						// treat o as literal
						List<Edge<Text, Text>> edges = this.getEdges();
						for (Edge<Text, Text> edge : edges) {
							if (edge.getValue().toString().equals(p)
									&& edge.getDestinationVertexID().toString()
											.equals(o)) {
								String var = s.substring(s.indexOf('?'),
										s.length());
								String queryCopy = query.replaceAll("<[?]"+ var + ">",
																	"<"+ this.getVertexID().toString() + ">");
								sendMessage(new Text("MasterNode"), new Text(queryCopy));
							}
						}
					} else if (isKnown(s) && isKnown(o)) {//s p o
						if (this.getVertexID().toString().equals(s)) {
							List<Edge<Text, Text>> edges = this.getEdges();
							for (Edge<Text, Text> edge : edges) {
								if (edge.getValue().toString().equals(p)
										&& edge.getDestinationVertexID()
												.toString().equals(o)) {
									String queryCopy = query;
									sendMessage(new Text("MasterNode"),
											new Text(queryCopy));
									break;
								}
							}
						}
					} else if (!isKnown(p)){//s ?p or ?p o
						if(this.getVertexID().toString().equals(s)){
							
						}
						
					}

				} else {// third++ superstep
					while (messages.hasNext()) {
						//String message = messages.next().toString();
						//String[] split = message.split("<>");

					}
				}
			} else {// what masternode should do is defined in this section.
				while (messages.hasNext()) {
					Text msg = messages.next();
					String value = this.getValue().toString();
					this.setValue(value.equals("") || value == null ? new Text(
							msg) : new Text(value + "<>" + msg));

				}
			}
		}

		public boolean isKnown(String SorPorO) {
			if (SorPorO.startsWith("?"))
				return false;
			else
				return true;
		}

		public String SelectOneTripleFromQuery(String WholeQuery) {
			// This method waits to complete.
			return WholeQuery;
		}
	}// calss vertex

	static class HamaBgpVertexReader extends
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
			String line = value.toString();

			Node[] nodes = null;
			try {
				nodes = NxParser.parseNodes(line);
			} catch (ParseException e) {
				// if there is a syntax error, we just log and ignore it.
				log.debug(e.getMessage());
				return false;
			}
			assert (nodes != null);

			String s = nodes[0].toString();
			String p = nodes[1].toString();
			String o = (nodes[2] instanceof Literal ? nodes[2].toN3()
					: nodes[2].toString());

			vertex.setVertexID(new Text(s));
			vertex.setValue(new Text(""));
			vertex.addEdge(new Edge<Text, Text>(new Text(o), new Text(p)));

			return true;
		}

	}

	public static GraphJob createJob(String[] args, HamaConfiguration conf)
			throws Exception {

		GraphJob myjob = new GraphJob(conf, HamaBgp.class);
		myjob.setJobName("mypagerank-hama");
		// myjob.set("query",
		// "<?s> <http://dbpedia.org/ontology/nearestCity> <http://dbpedia.org/resource/Middletown%2C_Orange_County%2C_New_York> .");
//		myjob.set(
//				"query",
//				"<http://dbpedia.org/resource/William_Dehning> <http://dbpedia.org/ontology/birthplace> <?someplace> .");
		myjob.set("query","<http://dbpedia.org/resource/The_Best_of_Chic%2C_Volume_2> <http://dbpedia.org/ontology/genre> <?o> .");
		myjob.setMaxIteration(2);
		myjob.setVertexClass(HamaBgpVertex.class);
		myjob.setVertexInputReaderClass(HamaBgpVertexReader.class);
		myjob.setInputPath(new Path(args[0]));
		myjob.setOutputPath(new Path(args[1]));

		//
		myjob.setInputKeyClass(LongWritable.class);
		myjob.setInputValueClass(Text.class);
		myjob.setInputFormat(TextInputFormat.class);
		myjob.setPartitioner(HashPartitioner.class);
		myjob.setOutputFormat(BgpTextOutputFormat.class);
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(DoubleWritable.class);
		//
		myjob.setVertexIDClass(Text.class);
		myjob.setVertexValueClass(Text.class);
		myjob.setEdgeValueClass(Text.class);
		return myjob;

	}

	public static void main(String[] args) throws Exception {

		HamaConfiguration conf = new HamaConfiguration(new Configuration());
		GraphJob myjob = createJob(args, conf);

		long startTime = System.currentTimeMillis();
		if (myjob.waitForCompletion(true)) {
			System.out.println("Job Finished in "
					+ (System.currentTimeMillis() - startTime) / 1000.0
					+ " seconds");
		}
	}

}
