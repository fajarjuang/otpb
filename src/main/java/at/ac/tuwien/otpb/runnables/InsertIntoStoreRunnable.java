package at.ac.tuwien.otpb.runnables;

import at.ac.tuwien.otpb.Changeset;
import at.ac.tuwien.otpb.ChangesetCreator;
import at.ac.tuwien.otpb.UpdateType;
import info.aduna.iteration.Iterations;
import org.openrdf.model.Statement;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;

import java.util.List;

/**
 * Transfers data from one store to another
 * used to insert data from the temporary store to the main store
 * creates the also the changeset
 */
public class InsertIntoStoreRunnable implements Runnable {
	private final RepositoryConnection source;
	private final RepositoryConnection target;
	private final boolean withChangeset;

	public InsertIntoStoreRunnable(RepositoryConnection source, RepositoryConnection target, boolean withChangeset) {
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
				List<Statement> oldStatements = Iterations.asList(target.getStatements(statement.getSubject(), statement.getPredicate(), null, false));
				if (withChangeset) {
					if (oldStatements.size() >= 1) {
						ChangesetCreator changesetCreator = new ChangesetCreator(target.getValueFactory(), oldStatements.get(0), statement);
						Changeset changeset = changesetCreator.createChangeset();
						target.add(changeset.createStatements());
					} else {
						ChangesetCreator changesetCreator = new ChangesetCreator(target.getValueFactory(), statement,
								UpdateType.INSERT);
						Changeset changeset = changesetCreator.createChangeset();
						target.add(changeset.createStatements());
					}
				}
				if (oldStatements.size() >= 1) {
					if (oldStatements.size() > 1) {
						System.out.println("warning: non unique statement");
					}
					//delete the old one
					target.remove(oldStatements.get(0));
				}
				target.add(statement);
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
