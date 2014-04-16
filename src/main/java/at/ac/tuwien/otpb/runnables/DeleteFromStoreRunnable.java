package at.ac.tuwien.otpb.runnables;

import at.ac.tuwien.otpb.Changeset;
import at.ac.tuwien.otpb.ChangesetCreator;
import at.ac.tuwien.otpb.UpdateType;
import org.openrdf.model.Statement;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;

public class DeleteFromStoreRunnable implements Runnable {
	private final RepositoryConnection source;
	private final RepositoryConnection target;
	private final boolean withChangeset;

	public DeleteFromStoreRunnable(RepositoryConnection source, RepositoryConnection target, boolean withChangeset) {
		this.source = source;
		this.target = target;
		this.withChangeset = withChangeset;
	}

	@Override
	public void run() {
		try {
			target.begin();
			RepositoryResult<Statement> statements = source.getStatements(null, null, null, false);
			while (statements.hasNext()) {
				Statement statement = statements.next();
				target.remove(statement);

				if (withChangeset) {
					ChangesetCreator changesetCreator = new ChangesetCreator(target.getValueFactory(), statement, UpdateType.DELETION);
					Changeset changeset = changesetCreator.createChangeset();
					target.add(changeset.createStatements());
				}
			}

			target.commit();
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
