package at.ac.tuwien.otpb.runnables;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.ResultSet;

public class QueryRunnable implements Runnable
{
    private QueryExecution query;
    private long resultCount = 0;

    public QueryRunnable(QueryExecution query) {
        this.query = query;
    }

    public long getResultCount() { return resultCount; }

    @Override
    public void run() {
        ResultSet result = null;
        try {
            result = query.execSelect();
            while(result.hasNext()) {
                result.next();
                resultCount++;
            }

        } finally {
            query.close();
        }
    }
}
