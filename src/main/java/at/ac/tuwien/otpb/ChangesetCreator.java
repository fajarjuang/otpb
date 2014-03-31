package at.ac.tuwien.otpb;


import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;

public class ChangesetCreator {
	private Statement oldStatement;
	private Statement newStatement;
	private UpdateType type;
	private ValueFactory factory;

	public ChangesetCreator(ValueFactory factory, Statement newOrDeletedStatement, UpdateType type) {
		this.factory = factory;
		this.newStatement = newOrDeletedStatement;
		this.type = type;
	}

	public ChangesetCreator(ValueFactory factory, Statement oldStatement, Statement newStatment) {
		this.oldStatement = oldStatement;
		this.newStatement = newStatment;
		this.type = UpdateType.UPDATE;
		this.factory = factory;
	}

	public Changeset createChangeset() {
		switch (type) {
			case UPDATE: {
				Changeset changeset = new Changeset(factory);
				changeset.setAddition(newStatement);
				changeset.setRemoval(oldStatement);
				return changeset;
			}
			case INSERT: {
				Changeset changeset = new Changeset(factory);
				changeset.setAddition(newStatement);
				return changeset;
			}
			case DELETION: {
				Changeset changeset = new Changeset(factory);
				changeset.setRemoval(newStatement);
				return changeset;
			}
		}

		return new Changeset(factory);
	}
}
