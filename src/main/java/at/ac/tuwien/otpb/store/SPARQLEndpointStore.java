package at.ac.tuwien.otpb.store;

import at.ac.tuwien.genben.xml.TestCase;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sparql.SPARQLRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;

import java.io.File;
import java.io.IOException;

public class SPARQLEndpointStore implements Store {
	private Repository repository;

	@Override
	public void initStore(TestCase testCase) {
		SPARQLRepository repository = new SPARQLRepository(testCase.getParameter("sparqlqueryurl").getValue(), testCase.getParameter("sparqlupdateurl").getValue());
		if (testCase.getParameter("sparqluser") != null && testCase.getParameter("sparqlpass") != null)
			repository.setUsernameAndPassword(testCase.getParameter("sparqluser").getValue(), testCase.getParameter("sparqlpass").getValue());
		this.repository = repository;
		try {
			repository.initialize();
		} catch (RepositoryException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void closeStore() {
		try {
			repository.shutDown();
		} catch (RepositoryException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void deleteStore() {
	}

	@Override
	public String[] getFileOrDirectoryList() {
		return new String[0];
	}

	@Override
	public void loadRdf(String filename, RDFFormat format) {
		RepositoryConnection repositoryConnection = getRepositoryConnection();

		try {
			repositoryConnection.begin();
			repositoryConnection.add(new File(filename), "", format);
			repositoryConnection.commit();
		} catch (RepositoryException | RDFParseException | IOException e) {
			System.err.println(filename);
			e.printStackTrace();
		} finally {
			try {
				repositoryConnection.close();
			} catch (RepositoryException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public RepositoryConnection getRepositoryConnection() {
		try {
			return repository.getConnection();
		} catch (RepositoryException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public RepositoryConnection getQueryConnection() {
		return getRepositoryConnection();
	}
}
