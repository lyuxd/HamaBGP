/*
 * this class can only process the simplest query.
 * such as ?s p o and s p ?o
 */
package hama.bgp;

import org.apache.log4j.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.TextInputFormat;

import org.apache.hama.graph.GraphJob;
import org.apache.hama.graph.GraphJobRunner;


public class HamaBgpComplete {

	private static final Logger log = Logger.getLogger(HamaBgpComplete.class);
	public static GraphJob createJob(String[] args, HamaConfiguration conf)
			throws Exception {
		conf.setBoolean(GraphJobRunner.GRAPH_REPAIR, true);
		GraphJob myjob = new GraphJob(conf, HamaBgpComplete.class);
		myjob.setJarByClass(HamaBgpComplete.class);
		myjob.setJobName("mypagerank-hama");
		
//		myjob.set(	"query","1<><http://dbpedia.org/resource/William_Mart%C3%ADnez> <http://dbpedia.org/ontology/clubs> <?o> .");
//		myjob.set("query","2<><http://dbpedia.org/resource/William_Dehning> <http://dbpedia.org/ontology/birthplace> <?someplace> .<><?someplace> <http://dbpedia.org/ontology/nearestCity> <?where> .");
//		myjob.set("query", "2<><http://dbpedia.org/resource/Aitkin> <http://dbpedia.org/ontology/nearestCity> <?where> .<><http://dbpedia.org/resource/William_Dehning> <http://dbpedia.org/ontology/birthplace> <?where> .");
		myjob.set("query", "2<><?book> <http://dbpedia.org/ontology/language> <?lang> .<><?book> <http://dbpedia.org/ontology/author> <http://dbpedia.org/resource/Tove_Jansson> .");
//		myjob.set("query", "1<><?album> <http://dbpedia.org/ontology/producer> <http://dbpedia.org/resource/The_Neptunes> .");
//		myjob.set(	"query","1<><http://dbpedia.org/resource/William_Dehning> <?p> <?o> .");
		log.info("query:"+conf.get("query"));

		myjob.setMaxIteration(4);
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
		//myjob.setOutputFormat(TextOutputFormat.class);
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
		log.info("Job begain.");
		if (myjob.waitForCompletion(true)) {
			System.out.println("Job Finished in "
					+ (System.currentTimeMillis() - startTime) / 1000.0
					+ " seconds");
		}
	}

}
