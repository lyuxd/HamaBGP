package hama.bgp;

import java.util.Comparator;


public class SubQueryWithScore {

	private String subQuery ;
	private int score;

	public String getSubQuery() {
		return subQuery;
	}

	public void setSubQuery(String SubQuery) {
		this.subQuery = SubQuery;
	}
	
	public void setSubQuery(String SubQuery,int Score){
		this.setSubQuery(SubQuery);
		this.setScore(Score);
	}

	public int getScore() {
		return score;
	}

	public void setScore(int Score) {
		this.score = Score;
	}
	

}
class MyCompare implements Comparator<SubQueryWithScore>{

	
	@Override
	public int compare(SubQueryWithScore subquery1, SubQueryWithScore subquery2) {
		return subquery1.getScore()>subquery2.getScore() ? -1: 1;
	
	}
	
}