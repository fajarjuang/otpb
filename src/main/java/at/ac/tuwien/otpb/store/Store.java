package at.ac.tuwien.otpb.store;

import at.ac.tuwien.genben.xml.TestCase;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.rio.RDFFormat;

/**
 * store interface for the test
 */
public interface Store {
	void initStore(TestCase testCase);

	void closeStore();

	void deleteStore();

	String[] getFileOrDirectoryList();

	void loadRdf(String filename, RDFFormat format);

	RepositoryConnection getRepositoryConnection();

	RepositoryConnection getQueryConnection();
}
