package at.ac.tuwien.otpb.runnables;

import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;


public class QueryRunnable implements Runnable {
	private TupleQuery query;
	private long resultCount = 0;

	public QueryRunnable(TupleQuery query) {
		this.query = query;
	}

	public long getResultCount() {
		return resultCount;
	}

	@Override
	public void run() {
		TupleQueryResult result = null;
		try {
			result = query.evaluate();
			while (result.hasNext()) {
				result.next();
				resultCount++;
			}
			result.close();
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		} finally {
			if (result != null) {
				try {
					result.close();
				} catch (QueryEvaluationException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
