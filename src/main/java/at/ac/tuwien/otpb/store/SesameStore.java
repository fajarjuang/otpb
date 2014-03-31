package at.ac.tuwien.otpb.store;

import at.ac.tuwien.genben.xml.TestCase;
import org.apache.commons.io.FileUtils;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.nativerdf.NativeStore;

import java.io.File;
import java.io.IOException;

public class SesameStore implements Store {
	private Repository repository;

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

	@Override
	public void initStore(TestCase testCase) {
		File file = new File("store/");
		repository = new SailRepository(new NativeStore(file));
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
		try {
			FileUtils.deleteDirectory(new File("store/"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public String[] getFileOrDirectoryList() {
		return new String[0];  //To change body of implemented methods use File | Settings | File Templates.
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
}
