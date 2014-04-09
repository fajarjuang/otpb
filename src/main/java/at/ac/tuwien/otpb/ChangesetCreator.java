package at.ac.tuwien.otpb;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Statement;

import java.util.ArrayList;
import java.util.List;

public class ChangesetCreator
{
    private Statement oldStatement;
    private Statement newStatement;
    private UpdateType type;
    private Model model;

    public ChangesetCreator(Model model, Statement newOrDeletedStatement, UpdateType type) {
        this.model = model;
        this.newStatement = newOrDeletedStatement;
        this.type = type;
    }

    public ChangesetCreator(Model factory, Statement oldStatement, Statement newStatment) {
        this.oldStatement = oldStatement;
        this.newStatement = newStatment;
        this.type = UpdateType.UPDATE;
        this.model = factory;
    }

    public Changeset createChangeset() {
        List<Statement> statements = new ArrayList<Statement>();
        switch (type) {
            case UPDATE:
            {
                Changeset changeset = new Changeset(model);
                changeset.setAddition(newStatement);
                changeset.setRemoval(oldStatement);
                return changeset;
            }
            case INSERT:
            {
                Changeset changeset = new Changeset(model);
                changeset.setAddition(newStatement);
                return changeset;
            }
            case DELETION:
            {
                Changeset changeset = new Changeset(model);
                changeset.setRemoval(newStatement);
                return changeset;
            }
        }

        return new Changeset(model);
    }
}
