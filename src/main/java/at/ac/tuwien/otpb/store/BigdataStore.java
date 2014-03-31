package at.ac.tuwien.otpb.store;

import at.ac.tuwien.genben.xml.TestCase;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.Options;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class BigdataStore implements Store {
	private File bigDataFile;
	private Repository repository;

	@Override
	public void initStore(TestCase testCase) {
		try {
			Properties properties = new Properties();

			bigDataFile = new File("store" + ".jnl");

			if (!bigDataFile.exists()) {
				properties.setProperty(Options.CREATE, "true");
			}
			properties.setProperty(Options.FILE, bigDataFile.getAbsolutePath());
			properties.setProperty(com.bigdata.rdf.store.AbstractTripleStore.Options.STATEMENT_IDENTIFIERS, "false");
			properties.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");
			properties.setProperty(Options.BUFFER_MODE, "DiskRW");
			properties.setProperty(Options.READ_ONLY, "false");
			properties.setProperty(Options.WRITE_CACHE_ENABLED, "true");
			properties.setProperty(com.bigdata.rdf.store.AbstractTripleStore.Options.TEXT_INDEX, "false");
			properties.setProperty(IndexMetadata.Options.WRITE_RETENTION_QUEUE_CAPACITY, "8000");
			properties.setProperty(com.bigdata.rdf.store.AbstractTripleStore.Options.DEFAULT_INLINE_DATE_TIMES, "true");
			properties.setProperty(com.bigdata.rdf.store.AbstractTripleStore.Options.DEFAULT_INLINE_XSD_DATATYPE_LITERALS, "true");
			properties.setProperty(com.bigdata.rdf.store.AbstractTripleStore.Options.AXIOMS_CLASS, "com.bigdata.rdf.axioms.RdfsAxioms");
			System.out.println("using bigdata file: " + bigDataFile.getAbsolutePath());

			BigdataSail sail = new BigdataSail(properties);
			repository = new BigdataSailRepository(sail);
			repository.initialize();
		} catch (Exception ex) {
			ex.printStackTrace();
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
		bigDataFile.delete();
	}

	@Override
	public String[] getFileOrDirectoryList() {
		String list[] = {bigDataFile.getAbsolutePath()};
		return list;
	}

	@Override
	public void loadRdf(String filename, RDFFormat format) {
		RepositoryConnection repositoryConnection = getRepositoryConnection();

		try {
			repositoryConnection.begin();
			repositoryConnection.add(new File(filename), "", format);
			repositoryConnection.commit();
		} catch (RepositoryException e) {
			e.printStackTrace();
		} catch (RDFParseException e) {
			e.printStackTrace();
		} catch (IOException e) {
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
		try {
			return ((BigdataSailRepository) repository).getReadOnlyConnection();
		} catch (RepositoryException e) {
			e.printStackTrace();
		}
		return null;
	}
}
