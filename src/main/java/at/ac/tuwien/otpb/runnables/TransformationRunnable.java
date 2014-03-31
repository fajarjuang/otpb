package at.ac.tuwien.otpb.runnables;

import org.openrdf.model.Statement;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

public class TransformationRunnable implements Runnable {
	private final RepositoryConnection source;
	private final RepositoryConnection target;
	private final GraphQuery query;

	public TransformationRunnable(GraphQuery query, RepositoryConnection source, RepositoryConnection target) {
		this.query = query;
		this.source = source;
		this.target = target;
	}

	@Override
	public void run() {
		try {
			target.begin();
			GraphQueryResult result = query.evaluate();
			while (result.hasNext()) {
				Statement statement = result.next();
				target.add(statement);
			}
			target.commit();
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		} catch (RepositoryException e) {
			e.printStackTrace();
		} finally {
			try {
				source.close();
				target.close();
			} catch (RepositoryException e) {
				e.printStackTrace();
			}
		}
	}
}
