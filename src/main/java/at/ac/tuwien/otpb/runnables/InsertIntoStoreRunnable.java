package at.ac.tuwien.otpb.runnables;

import at.ac.tuwien.otpb.Changeset;
import at.ac.tuwien.otpb.ChangesetCreator;
import at.ac.tuwien.otpb.UpdateType;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;


import java.util.List;

/**
 * Transfers data from one store to another
 * used to insert data from the temporary store to the main store
 * creates the also the changeset
 */
public class InsertIntoStoreRunnable implements Runnable {
    private Model source;
    private Model target;
    private boolean withChangeset;

    public InsertIntoStoreRunnable(Model source, Model target, boolean withChangeset) {
        this.source = source;
        this.target = target;
        this.withChangeset = withChangeset;
    }

    @Override
    public void run() {
        StmtIterator statements = source.listStatements();
        while (statements.hasNext()) {
            Statement statement = statements.next();
            List<Statement> oldStatements = target.listStatements(statement.getSubject(),
                    statement.getPredicate(), (RDFNode) null).toList();

            if (withChangeset) {
                if (oldStatements.size() >= 1) {
                    ChangesetCreator changesetCreator = new ChangesetCreator(target, oldStatements.get(0), statement);
                    Changeset changeset = changesetCreator.createChangeset();
                    for (Statement changesetStatement : changeset.createStatements()) {
                        target.add(changesetStatement);
                    }
                } else {
                    ChangesetCreator changesetCreator = new ChangesetCreator(target, statement,
                            UpdateType.INSERT);
                    Changeset changeset = changesetCreator.createChangeset();
                    for (Statement changesetStatement : changeset.createStatements()) {
                        target.add(changesetStatement);
                    }
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
    }
}
