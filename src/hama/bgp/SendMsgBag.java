package hama.bgp;

public class SendMsgBag {
	public SendMsgBag(String destinationVertexId, String newQuery) {
		super();
		DestinationVertexId = destinationVertexId;
		NewQuery = newQuery;
	}
	public SendMsgBag() {
		
	}
	
	private String DestinationVertexId;
	private String NewQuery;
	
	public String getNewQuery() {
		return NewQuery;
	}
	public void setNewQuery(String newQuery) {
		NewQuery = newQuery;
	}
	public String getDestinationVertexId() {
		return DestinationVertexId;
	}
	public void setDestinationVertexId(String destinationVertexId) {
		DestinationVertexId = destinationVertexId;
	}

}
