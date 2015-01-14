package at.ac.tuwien.otpb.store;

import java.io.File;
import java.net.MalformedURLException;
import java.util.List;
import java.util.Vector;

import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;

import virtuoso.sesame2.driver.VirtuosoRepository;

public class TestStore {

    public static void log(String mess) {
        System.out.println("   " + mess);
    }

    public static void main(String[] args) throws RepositoryException, MalformedURLException {
        Repository repository = new VirtuosoRepository("jdbc:virtuoso://localhost:1111", "dba", "dba");
        repository.initialize();
        RepositoryConnection con = repository.getConnection();

        String query = null;
        URI context = repository.getValueFactory().createURI("http://juang.me/demo#this");
        Value[][] results = null;

        // test query data
        String fstr = "virtuoso_driver" + File.separator + "data.nt";
        log("Loading data from file: " + fstr);
        try {
            File dataFile = new File(fstr);
            con.add(dataFile, "", RDFFormat.NTRIPLES, context);
            query = "SELECT * FROM <" + context + "> WHERE {?s ?p ?o} LIMIT 1";
            results = doTupleQuery(con, query);
            log(results.toString());
        } catch (Exception e) {
            log("Error[" + e + "]");
            e.printStackTrace();
        }

    }

    private static Value[][] doTupleQuery(RepositoryConnection con, String query) throws RepositoryException,
        MalformedQueryException, QueryEvaluationException {
        TupleQuery resultsTable = con.prepareTupleQuery(QueryLanguage.SPARQL, query);
        TupleQueryResult bindings = resultsTable.evaluate();

        Vector<Value[]> results = new Vector<Value[]>();
        for (int row = 0; bindings.hasNext(); row++) {
            // System.out.println("RESULT " + (row + 1) + ": ");
            BindingSet pairs = bindings.next();
            List<String> names = bindings.getBindingNames();
            Value[] rv = new Value[names.size()];
            for (int i = 0; i < names.size(); i++) {
                String name = names.get(i);
                Value value = pairs.getValue(name);
                rv[i] = value;
                // if(column > 0) System.out.print(", ");
                // System.out.println("\t" + name + "=" + value);
                // vars.add(value);
                // if(column + 1 == names.size()) System.out.println(";");
            }
            results.add(rv);
        }
        return (Value[][]) results.toArray(new Value[0][0]);
    }
}
