package at.ac.tuwien.otpb.runnables;

import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;

import java.io.File;
import java.io.IOException;

public class ImportFileRunnable implements Runnable {
	private final RepositoryConnection connection;
	private final File file;

	public ImportFileRunnable(RepositoryConnection connection, File file) {
		this.connection = connection;
		this.file = file;
	}

	@Override
	public void run() {
		try {
			connection.begin();
			connection.add(file, "", RDFFormat.TURTLE);
			connection.commit();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (RDFParseException e) {
			e.printStackTrace();
		} catch (RepositoryException e) {
			e.printStackTrace();
		} finally {
			try {
				connection.close();
			} catch (RepositoryException e) {
				e.printStackTrace();
			}
		}
	}
}
