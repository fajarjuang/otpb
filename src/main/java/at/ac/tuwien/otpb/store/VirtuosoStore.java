package at.ac.tuwien.otpb.store;

import java.io.File;
import java.io.IOException;

import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;

import virtuoso.sesame2.driver.VirtuosoRepository;
import at.ac.tuwien.genben.xml.TestCase;

public class VirtuosoStore implements Store {
    private Repository myRepository;

    @Override
    public void initStore(TestCase testCase) {
        try {
            myRepository =
                new VirtuosoRepository(testCase.getParameter("url").getValue(), testCase.getParameter("username")
                        .getValue(), testCase.getParameter("password").getValue());
            myRepository.initialize();
        } catch (RepositoryException e) {
            System.out.println("initRepo Failed");
            e.printStackTrace();
        }
    }

    @Override
    public void closeStore() {
        try {
            myRepository.shutDown();
        } catch (RepositoryException e) {
            System.out.println("closeRepo Failed");
            e.printStackTrace();
        }

    }

    @Override
    public void deleteStore() {
        RepositoryConnection con = getRepositoryConnection();
        try {
            con.clear();
        } catch (RepositoryException e) {
            System.out.println("deleteRepo Failed");
            e.printStackTrace();
        }

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
            return myRepository.getConnection();
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
