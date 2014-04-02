package at.ac.tuwien.otpb.store;

import at.ac.tuwien.genben.xml.TestCase;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.http.HTTPRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;

import java.io.File;
import java.io.IOException;

public class RemoteSesameStore implements Store {
	private Repository repository;

	@Override
	public void initStore(TestCase testCase) {
		repository = new HTTPRepository(testCase.getParameter("url").getValue(), testCase.getParameter("repository").getValue());
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
