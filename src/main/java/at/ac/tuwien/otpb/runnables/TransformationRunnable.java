package at.ac.tuwien.otpb.runnables;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;

import java.util.List;

public class TransformationRunnable implements Runnable
{
    private Model source;
    private Model target;
    private QueryExecution query;

    public TransformationRunnable(QueryExecution query, Model source, Model target) {
        this.query = query;
        this.source = source;
        this.target = target;
    }

    @Override
    public void run() {
        try {
            Model model = query.execConstruct();
            StmtIterator result = model.listStatements();
            while (result.hasNext()) {
                Statement statement = result.next();
                target.add(statement);
            }
        } finally {
            query.close();
        }
    }
}