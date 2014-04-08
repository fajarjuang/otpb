package at.ac.tuwien.otpb;

import at.ac.tuwien.otpb.BenchmarkDriver;
import com.hp.hpl.jena.rdf.model.AnonId;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.vocabulary.RDF;

import java.util.ArrayList;
import java.util.List;

public class Changeset
{
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
    private Model model;
    private Resource changesetURI;

    public Changeset(Model model) {
        timestamp = BenchmarkDriver.DUMMY_TIME;
        this.model = model;
        changesetURI = model.createResource(ns + "changeset" + id++);

    }

    /** needed to link it to the commit */
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
        statements.add(model.createStatement(changesetURI, RDF.type, model.createResource(changesetTypeURI)));
        statements.add(model.createStatement(changesetURI, model.createProperty(createdDateURI),
                model.createResource(ns + "commit_ " + timestamp)));

        if (addition != null) {
            Resource additionBnode = model.createResource(AnonId.create("addition"));
            statements.add(model.createStatement(changesetURI, model.createProperty(subjectOfChangeURI), addition.getSubject()));
            statements.add(model.createStatement(changesetURI, model.createProperty(additionURI), additionBnode));
            statements.add(model.createStatement(additionBnode, RDF.subject, addition.getSubject()));
            statements.add(model.createStatement(additionBnode, RDF.predicate, addition.getPredicate()));
            statements.add(model.createStatement(additionBnode, RDF.object, addition.getObject()));
        }
        if (removal != null) {
            Resource removalBnode = model.createResource(AnonId.create("removal"));
            statements.add(model.createStatement(changesetURI, model.createProperty(subjectOfChangeURI), removal.getSubject()));
            statements.add(model.createStatement(changesetURI, model.createProperty(removalURI), removalBnode));
            statements.add(model.createStatement(removalBnode, RDF.subject, removal.getSubject()));
            statements.add(model.createStatement(removalBnode, RDF.predicate, removal.getPredicate()));
            statements.add(model.createStatement(removalBnode, RDF.object, removal.getObject()));
        }
        return statements;
    }
}
