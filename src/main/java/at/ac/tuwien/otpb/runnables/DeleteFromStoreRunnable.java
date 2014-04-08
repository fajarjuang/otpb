package at.ac.tuwien.otpb.runnables;

import at.ac.tuwien.Changeset;
import at.ac.tuwien.ChangesetCreator;
import at.ac.tuwien.UpdateType;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;

public class DeleteFromStoreRunnable implements Runnable {
    private Model source;
    private Model target;
    private boolean withChangeset;

    public DeleteFromStoreRunnable(Model source, Model target, boolean withChangeset) {
        this.source = source;
        this.target = target;
        this.withChangeset = withChangeset;
    }

    @Override
    public void run() {
        StmtIterator statements = source.listStatements();
        while (statements.hasNext()) {
            Statement statement = statements.next();
            target.remove(statement);
            if (withChangeset) {
                ChangesetCreator changesetCreator = new ChangesetCreator(target, statement, UpdateType.DELETION);
                Changeset changeset = changesetCreator.createChangeset();
                for (Statement changesetStatement : changeset.createStatements()) {
                    target.add(changesetStatement);
                }
            }
        }
        target.commit();
    }
}
