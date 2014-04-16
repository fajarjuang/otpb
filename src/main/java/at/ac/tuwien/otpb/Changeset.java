package at.ac.tuwien.otpb;


import org.openrdf.model.BNode;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDF;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


public class Changeset {
	private static long id;
	private static String ns = "http://purl.org/vocab/changeset/schema#";
	private static String changesetTypeURI = ns + "ChangeSet";
	private static String subjectOfChangeURI = ns + "subjectOfChange";
	private static String createdDateURI = ns + "createdDate";
	private static String additionURI = ns + "addition";
	private static String removalURI = ns + "removal";

	private Statement addition;
	private Statement removal;
	private long timestamp;
	private ValueFactory valueFactory;
	private URI changesetURI;

	public Changeset(ValueFactory valueFactory) {
		timestamp = BenchmarkDriver.DUMMY_TIME;
		this.valueFactory = valueFactory;
		changesetURI = valueFactory.createURI(ns + "changeset" + id++);

	}

	/**
	 * needed to link it to the commit
	 */
	public Resource getChangesetResource() {
		return changesetURI;
	}

	public void setAddition(Statement addition) {
		this.addition = addition;
	}

	public void setRemoval(Statement removal) {
		this.removal = removal;
	}

	public List<Statement> createStatements() {
		List<Statement> statements = new ArrayList<>();
		statements.add(valueFactory.createStatement(changesetURI, RDF.TYPE, valueFactory.createURI(changesetTypeURI)));
		statements.add(valueFactory.createStatement(changesetURI, valueFactory.createURI(createdDateURI),
				valueFactory.createURI(ns + "commit_" + timestamp)));

		if (addition != null) {
			BNode additionBnode = valueFactory.createBNode("addition" + UUID.randomUUID());
			statements.add(valueFactory.createStatement(changesetURI, valueFactory.createURI(subjectOfChangeURI), addition.getSubject()));
			statements.add(valueFactory.createStatement(changesetURI, valueFactory.createURI(additionURI), additionBnode));
			statements.add(valueFactory.createStatement(additionBnode, RDF.SUBJECT, addition.getSubject()));
			statements.add(valueFactory.createStatement(additionBnode, RDF.PREDICATE, addition.getPredicate()));
			statements.add(valueFactory.createStatement(additionBnode, RDF.OBJECT, addition.getObject()));
		}
		if (removal != null) {
			BNode removalBnode = valueFactory.createBNode("removal" + UUID.randomUUID());
			statements.add(valueFactory.createStatement(changesetURI, valueFactory.createURI(subjectOfChangeURI), removal.getSubject()));
			statements.add(valueFactory.createStatement(changesetURI, valueFactory.createURI(removalURI), removalBnode));
			statements.add(valueFactory.createStatement(removalBnode, RDF.SUBJECT, removal.getSubject()));
			statements.add(valueFactory.createStatement(removalBnode, RDF.PREDICATE, removal.getPredicate()));
			statements.add(valueFactory.createStatement(removalBnode, RDF.OBJECT, removal.getObject()));
		}
		return statements;
	}
}
