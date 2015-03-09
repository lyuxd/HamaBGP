package hama.bgp;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.Vertex;
import org.apache.log4j.Logger;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;

public class HamaBgpVertex extends Vertex<Text, Text, Text> {
	private static final Logger log = Logger.getLogger(HamaBgpVertex.class);
	String query;

	@Override
	public void setup(Configuration conf) {
		query = conf.get("query");
	}

	@Override
	public void compute(Iterator<Text> messages) throws IOException {

		if (!this.getVertexID().toString().equals("MasterNode")) {
			// the first SuperStep, send message to son then sleep.
			if (this.getSuperstepCount() == 0) {
//				List<Edge<Text, Text>> edges = this.getEdges();
//				for (Edge<Text, Text> edge : edges) {
//					this.sendMessage(edge, new Text(this.getVertexID() + "<>"
//							+ edge.getValue()));
//				}
				voteToHalt();
			// the second SuperStep, update vertex's value then sleep.
//			} else if (this.getSuperstepCount() == 1) {
//				log.info("updating values...");
//				String value = "";
//				while (messages.hasNext()) {
//					String msg = messages.next().toString();
//					if (value.equals("") || value == null) {
//						value = msg;
//					} else {
//						value = value + "<>" + msg;
//					}
//				}
//				this.setValue(new Text(value));
//				voteToHalt();
			// after second SuperStep, compute the query.
			} else{
				while (messages.hasNext()) {
					String queryCopy = messages.next().toString();
					String subQuery = selectOneTripleFromQuery(queryCopy);
					matchSingleTriple(subQuery, queryCopy);
				}
				voteToHalt();
			}
			// MasterNode's duty. what MasterNode should do is defined in this section.
		} else {
			if (this.getSuperstepCount() > 0) {
				log.info(this.getSuperstepCount() + " MasterNode.");
				while (messages.hasNext()) {
					Text msg = messages.next();
					String value = this.getValue().toString();
					this.setValue(value.equals("") || value == null ? new Text(
							"\n\n\n"+msg) : new Text(value + "\n\n\n" + msg));
				}
				voteToHalt();
			} else if (this.getSuperstepCount() == 0) {
				log.info(this.getSuperstepCount() + " masternode.");
				String queryCopy = query;
				//System.out.println(queryCopy);
				try {
					SendMsgBag sendMsgBag = sortTempQuery(queryCopy);
					log.info(this.getSuperstepCount() + "  "
							+ this.getVertexID() + " SEND "
							+ sendMsgBag.getNewQuery() + " TO "
							+ sendMsgBag.getDestinationVertexId());
					sendMessage(new Text(sendMsgBag.getDestinationVertexId()),
							new Text(sendMsgBag.getNewQuery()));
				} catch (ParseException e) {

					log.info(e.getMessage());
				}
				// voteToHalt();
			}
		}
	}

	public boolean isKnown(String SorPorO) {
		if (SorPorO.startsWith("?"))
			return false;
		else
			return true;
	}

	/*
	 * Select the unverified triple with the highest score.
	 */
	public String selectOneTripleFromQuery(String WholeQuery) {
		String[] split = WholeQuery.split("<>");
		int splitLength = split.length;
		int numTripleNotProcess = Integer.valueOf(split[0]);
		return split[splitLength - numTripleNotProcess];
	}

	/*
	 * matchSingleTriple() is the most important method. It is designed to solve
	 * single triple pattern:
	 * 
	 * S P ?O 
	 * ?S P O 
	 * S P O 
	 * S ?P O 
	 * ?S ?P O
	 * S ?P ?O
	 * ?S P ?O( not implements )
	 */
	public void matchSingleTriple(String Triple, String QueryCopy)
			throws IOException {
		Node nodes[] = null;
		try {
			nodes = NxParser.parseNodes(Triple);
		} catch (ParseException e) {

			// if there is a syntax error, we just log and ignore it.
			log.debug(e.getMessage());
		}

		String s = nodes[0].toString();
		String p = nodes[1].toString();
		String o = (nodes[2] instanceof Literal ? nodes[2].toN3() : nodes[2]
				.toString());

		// CASE 1 : s p ?o
		if (isKnown(s) && isKnown(p) && !isKnown(o)) {
			// My name is "s".
			if (this.getVertexID().toString().equals(s)) {
				List<Edge<Text, Text>> edges = this.getEdges();
				for (Edge<Text, Text> edge : edges) {
					// I have a edge whose value is "P".
					if (edge.getValue().toString().equals(p)) {
						String queryCopyAfterReplace = bindingVar(QueryCopy, o, edge.getDestinationVertexID().toString());
						try {
							SendMsgBag sendMsgBag = sortTempQuery(queryCopyAfterReplace);
							log.info(this.getSuperstepCount() + "  "
									+ this.getVertexID() + " SEND "
									+ sendMsgBag.getNewQuery() + " TO "
									+ sendMsgBag.getDestinationVertexId());
							sendMessage(
									new Text(sendMsgBag
											.getDestinationVertexId()),
									new Text(sendMsgBag.getNewQuery()));
						} catch (ParseException e) {

							log.debug(e.getMessage());
						}

					}
				}
			} else
				voteToHalt();
			// CASE 2 : ?s p o
		} else if ((!isKnown(s)) && isKnown(p) && isKnown(o)) {
			// My name is "o".
			if (this.getVertexID().toString().equals(o)) {
				String[] oppEdges = this.getValue().toString().split("<>");
				int oppEdgesLength = oppEdges.length;
				for (int i = 0; i + 1 < oppEdgesLength; i+=2) {
					if (oppEdges[ i + 1].equals(p)) {
						String queryCopyAfterReplace = bindingVar(QueryCopy,s,oppEdges[ i]);
						try {
							SendMsgBag sendMsgBag = sortTempQuery(queryCopyAfterReplace);
							log.info(this.getSuperstepCount() + "  "
									+ this.getVertexID() + " SEND "
									+ sendMsgBag.getNewQuery() + " TO "
									+ sendMsgBag.getDestinationVertexId());
							sendMessage(
									new Text(sendMsgBag
											.getDestinationVertexId()),
									new Text(sendMsgBag.getNewQuery()));
						} catch (ParseException e) {

							log.debug(e.getMessage());
						}
					}
				}
			} else
				voteToHalt();
			// CASE 3 : s p o
		} else if (isKnown(s) && isKnown(p) && isKnown(o)) {
			// my name is "s".
			if (this.getVertexID().toString().equals(s)) {
				List<Edge<Text, Text>> edges = this.getEdges();
				for (Edge<Text, Text> edge : edges) {
					if (edge.getValue().toString().equals(p)
							&& edge.getDestinationVertexID().toString()
									.equals(o)) {
						String queryCopyAfterReplace = QueryCopy;
						try {
							SendMsgBag sendMsgBag = sortTempQuery(queryCopyAfterReplace);
							log.info(this.getSuperstepCount() + "  "
									+ this.getVertexID() + " SEND "
									+ sendMsgBag.getNewQuery() + " TO "
									+ sendMsgBag.getDestinationVertexId());
							sendMessage(
									new Text(sendMsgBag
											.getDestinationVertexId()),
									new Text(sendMsgBag.getNewQuery()));
						} catch (ParseException e) {

							log.debug(e.getMessage());
						}
						break;
					} 
				}
			} else
				voteToHalt();
			// CASE 4 : s ?p o
		} else if(isKnown(s) && !isKnown(p) && isKnown(o)){
			// my name is "s".
			if (this.getVertexID().toString().equals(s)) {
				List<Edge<Text, Text>> edges = this.getEdges();
				for (Edge<Text, Text> edge : edges) {
					if (edge.getDestinationVertexID().toString()
									.equals(o)) {
						String queryCopyAfterReplace = bindingVar(QueryCopy,p,edge.getValue().toString());
						try {
							SendMsgBag sendMsgBag = sortTempQuery(queryCopyAfterReplace);
							log.info(this.getSuperstepCount() + "  "
									+ this.getVertexID() + " SEND "
									+ sendMsgBag.getNewQuery() + " TO "
									+ sendMsgBag.getDestinationVertexId());
							sendMessage(
									new Text(sendMsgBag
											.getDestinationVertexId()),
									new Text(sendMsgBag.getNewQuery()));
						} catch (ParseException e) {

							log.debug(e.getMessage());
						}
						break;
					} 
				}
			} else
				voteToHalt();
			// CASE 5 : ?s ?p o
		} else if(!isKnown(s) && !isKnown(p) && isKnown(o)){
			// my name is "o".
			if (this.getVertexID().toString().equals(o)) {
				String[] oppEdges = this.getValue().toString().split("<>");
				int oppEdgesLength = oppEdges.length;
				for (int i = 0; i + 1< oppEdgesLength; i+=2) {
						String queryCopyAfterReplace = bindingVar(QueryCopy,s,oppEdges[i]);
						queryCopyAfterReplace = bindingVar(queryCopyAfterReplace,p,oppEdges[ i + 1]);
						try {
							SendMsgBag sendMsgBag = sortTempQuery(queryCopyAfterReplace);
							log.info(this.getSuperstepCount() + "  "
									+ this.getVertexID() + " SEND "
									+ sendMsgBag.getNewQuery() + " TO "
									+ sendMsgBag.getDestinationVertexId());
							sendMessage(
									new Text(sendMsgBag
											.getDestinationVertexId()),
									new Text(sendMsgBag.getNewQuery()));
						} catch (ParseException e) {

							log.debug(e.getMessage());
						}
				}
			} else
				voteToHalt();
			// CASE 6 : s ?p ?o
		} else if(isKnown(s) && !isKnown(p) && !isKnown(o)){
			// My name is "s".
			if (this.getVertexID().toString().equals(s)) {
				List<Edge<Text, Text>> edges = this.getEdges();
				for (Edge<Text, Text> edge : edges) {
					// I have a edge whose value is "P".
					String queryCopyAfterReplace = bindingVar(QueryCopy,p,edge.getValue().toString());
					queryCopyAfterReplace = bindingVar(queryCopyAfterReplace,o,edge.getDestinationVertexID().toString());
					try {
						SendMsgBag sendMsgBag = sortTempQuery(queryCopyAfterReplace);
						log.info(this.getSuperstepCount() + "  "
								+ this.getVertexID() + " SEND "
								+ sendMsgBag.getNewQuery() + " TO "
								+ sendMsgBag.getDestinationVertexId());
						sendMessage(
								new Text(sendMsgBag.getDestinationVertexId()),
								new Text(sendMsgBag.getNewQuery()));
					} catch (ParseException e) {
						log.debug(e.getMessage());
					}
				}
			} else
				voteToHalt();
		}

	}

	/*
	 * sortTempQuery(NewQueryCopy)
	 *  input: 3<><!><!><!> .<?><p><o> .<s><p><?> .
	 * output: 2<><!><!><!> .<s><p><?> .<?><P><o> . 
	 * This method is to receive the query with N edges which M of them are not verified.
	 *Process : The head N-M verified edges are stay in their original positions while 
	 *the rest ones are sorted by a standard rule, simplest, score as below.
	 */
	public String bindingVar(String Query, String VarWithBrackets, String Value){
		String Var = VarWithBrackets.substring(VarWithBrackets.indexOf('?'), VarWithBrackets.length());
		return Query.replaceAll("<[?]"+Var+">", "<"+Value+">");
	}

	public SendMsgBag sortTempQuery(String NewQueryCopy) throws ParseException {
		String destination = null;
		String[] split = NewQueryCopy.split("<>");
		int splitLength = split.length;
		int TTL = Integer.valueOf(split[0]);
		int newTTL = TTL;
		if (!this.getVertexID().toString().equals("MasterNode")) {
			newTTL = TTL - 1;
			if (newTTL == 0)
				return new SendMsgBag("MasterNode", NewQueryCopy);
		}
		String newQuery = "" + newTTL;
		for (int i = 1; i < splitLength - newTTL; i++) {
			newQuery = newQuery + "<>" + split[i];
		}
		Vector<SubQueryWithScore> subQueryVector = new Vector<SubQueryWithScore>();
		for (int i = splitLength - newTTL; i < splitLength; i++) {
			try {
				int score = 0;
				Node[] nodes = NxParser.parseNodes(split[i].toString());
				String s = nodes[0].toString();
				String p = nodes[1].toString();
				String o = (nodes[2] instanceof Literal ? nodes[2].toN3()
						: nodes[2].toString());
				if (!s.startsWith("?"))
					score += 2;
				if (!p.startsWith("?"))
					score += 1;
				if (!o.startsWith("?"))
					score += 1;
				SubQueryWithScore subQuerytmp = new SubQueryWithScore();
				subQuerytmp.setSubQuery(split[i].toString(), score);
				subQueryVector.addElement(subQuerytmp);
			} catch (ParseException e) {
				// if there is a syntax error, we just log and ignore it.
				log.debug(e.getMessage());
			}
		}
		Comparator<SubQueryWithScore> cp = new MyCompare();
		Collections.sort(subQueryVector, cp);
		Node[] nodes = NxParser.parseNodes(subQueryVector.get(0).getSubQuery()
				.toString());
		String s = nodes[0].toString();
		// p is not used temporarily for the function of querying "?p" is not
		// implemented.
		String p = nodes[1].toString();
		String o = (nodes[2] instanceof Literal ? nodes[2].toN3() : nodes[2]
				.toString());
		if (!s.startsWith("?"))
			destination = s;
		else if (!o.startsWith("?"))
			destination = o;
		else if(!p.startsWith("?")){
			// if ?S P ?O, oops, it is not implemented.
		}
		for (int i = 0; i < subQueryVector.size(); i++) {
			newQuery = newQuery + "<>" + subQueryVector.get(i).getSubQuery();
		}
		SendMsgBag msgBag = new SendMsgBag(destination, newQuery);
		return msgBag;

	}
}// class vertex