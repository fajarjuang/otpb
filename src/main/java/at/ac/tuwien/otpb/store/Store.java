package at.ac.tuwien.otpb.store;

import at.ac.tuwien.genben.xml.TestCase;
import com.hp.hpl.jena.rdf.model.Model;
import org.apache.jena.riot.RDFFormat;

/**
 * store interface for the test
 */
public interface Store
{
    void initStore(TestCase testCase);
    void closeStore();
    void deleteStore();
    String[] getFileOrDirectoryList();
    void loadRdf(String filename, RDFFormat format);
    Model getRepositoryConnection();
    Model getQueryConnection();
}
