package at.ac.tuwien.otpb;

import at.ac.tuwien.otpb.runnables.DeleteFromStoreRunnable;
import at.ac.tuwien.otpb.runnables.ImportFileRunnable;
import at.ac.tuwien.otpb.runnables.InsertIntoStoreRunnable;
import at.ac.tuwien.otpb.runnables.QueryRunnable;
import at.ac.tuwien.otpb.runnables.TransformationRunnable;
import at.ac.tuwien.otpb.store.Store;
import at.ac.tuwien.genben.TestDriver;
import at.ac.tuwien.genben.sensors.FileSizeSensor;
import at.ac.tuwien.genben.sensors.MemorySensor;
import at.ac.tuwien.genben.sensors.TimeSensor;
import at.ac.tuwien.genben.sensors.ValueSensor;
import at.ac.tuwien.genben.xml.TestCase;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.memory.MemoryStore;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BenchmarkDriver implements TestDriver {
	private Store store;
	private TimeSensor importInsertsFileSensor = new TimeSensor("_1_ImportTemporaryInsertEplan");
	private TimeSensor insertInsertsIntoStoreSensor = new TimeSensor("_2_Insert_Eplan_Bigdata");
	private TimeSensor insertsTransformEplanToVCDM = new TimeSensor("_3_Transform_Insert_Eplan_To_VCDM");
	private TimeSensor inserstInsertEplanToVCDMTranformation = new TimeSensor("_4_Insert_VCDM_Bigdata");
	private TimeSensor insertTranfsformVCDMToOPM = new TimeSensor("_5_Transform_Insert_VCDM_to_OPM");
	private TimeSensor insertInsertVCDMToOPMTransformation = new TimeSensor("_6_Insert_OPM_Bigdata");
	private TimeSensor importDeletionsFileSensor = new TimeSensor("_14_Import_Deletions_Eplan");
	private TimeSensor removeEplanDeletionsSensor = new TimeSensor("_15_Write_Deletions_Eplan_Bigdata");
	private TimeSensor transformDeletionsEplanToVCDMSensor = new TimeSensor("_16_Transform_Deletions_Eplan_To_VCDM");
	private TimeSensor removeVCDMDeletionsSensor = new TimeSensor("_17_Write_Deletions_VCDM_Bigdata");
	private TimeSensor transformDeletionsVCDMToOP = new TimeSensor("_18_Transform_Deletions_VCDM_To_OPM");
	private TimeSensor removeOPMDeletionsSensor = new TimeSensor("_19_Write_Deletions_OPM_Bigdata");
	private TimeSensor importUpdatesFileSensor = new TimeSensor("_7_Import_Updates_Eplan");
	private TimeSensor writeEplanUpdatesSensor = new TimeSensor("_9_Write_Updates_Eplan_Bigdata");
	private TimeSensor transformUpdatesEplanToVCDMSensor = new TimeSensor("_10_Transform_Updates_Eplan_To_VCDM");
	private TimeSensor writeVCDMUpdatesSensor = new TimeSensor("_11_Write_VCDM_Updates_Bigdata");
	private TimeSensor transformUpdatesVCDMToOPMSensor = new TimeSensor("_12_Transform_Updates_VCDM_OPM");
	private TimeSensor writeOPMUpdatesSensor = new TimeSensor("_13_Write_OPM_Updates_Bigdata");
	private TimeSensor query1Sensor = new TimeSensor("Query1");
	private TimeSensor query2_1Sensor = new TimeSensor("Query2_1");
	private TimeSensor query2_2Sensor = new TimeSensor("Query2_2");
	private TimeSensor query2_3Sensor = new TimeSensor("Query2_3");
	private TimeSensor query3Sensor = new TimeSensor("Query3");
	private TimeSensor query4Sensor = new TimeSensor("Query4");
	private TimeSensor query5Sensor = new TimeSensor("Query5");
	private TimeSensor query6Sensor = new TimeSensor("Query6");
	private TimeSensor query7Sensor = new TimeSensor("Query7");
	private TimeSensor query8Sensor = new TimeSensor("Query8");
	private MemorySensor memorySensor = new MemorySensor("MemoryUsed");
	private List<TimeSensor> timeSensors = new ArrayList<>();
	private boolean versioned = true;
	public static long DUMMY_TIME = 0;

	public BenchmarkDriver() {
		timeSensors.add(importInsertsFileSensor);
		timeSensors.add(insertInsertsIntoStoreSensor);
		timeSensors.add(insertsTransformEplanToVCDM);
		timeSensors.add(inserstInsertEplanToVCDMTranformation);
		timeSensors.add(insertTranfsformVCDMToOPM);
		timeSensors.add(insertInsertVCDMToOPMTransformation);
		timeSensors.add(importDeletionsFileSensor);
		timeSensors.add(removeEplanDeletionsSensor);
		timeSensors.add(transformDeletionsEplanToVCDMSensor);
		timeSensors.add(removeVCDMDeletionsSensor);
		timeSensors.add(transformDeletionsVCDMToOP);
		timeSensors.add(removeOPMDeletionsSensor);
		timeSensors.add(importUpdatesFileSensor);
		timeSensors.add(writeEplanUpdatesSensor);
		timeSensors.add(transformUpdatesEplanToVCDMSensor);
		timeSensors.add(writeVCDMUpdatesSensor);
		timeSensors.add(transformUpdatesVCDMToOPMSensor);
		timeSensors.add(writeOPMUpdatesSensor);
		timeSensors.add(query1Sensor);
		timeSensors.add(query2_1Sensor);
		timeSensors.add(query2_2Sensor);
		timeSensors.add(query2_3Sensor);
		timeSensors.add(query3Sensor);
		timeSensors.add(query4Sensor);
		timeSensors.add(query5Sensor);
		timeSensors.add(query6Sensor);
		timeSensors.add(query7Sensor);
		timeSensors.add(query8Sensor);
	}

	@Override
	public void prepare(TestCase testCase) {
		String storeClassName = testCase.getParameter("store").getValue();
		try {
			store = (Store) Class.forName(storeClassName).newInstance();
		} catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
			e.printStackTrace();
		}

		versioned = testCase.getParameter("versioned").getBooleanValue();
		store.initStore(testCase);
		store.loadRdf(testCase.getParameter("eplanOWL").getValue(), RDFFormat.RDFXML);
		store.loadRdf(testCase.getParameter("opmOWL").getValue(), RDFFormat.RDFXML);
		store.loadRdf(testCase.getParameter("vcdmOWL").getValue(), RDFFormat.RDFXML);
	}

	@Override
	public void warmup(TestCase testCase) {
		//no warmup phase
	}

	void runQuery(String query, TimeSensor sensor) throws MalformedQueryException, RepositoryException {
		query = query.replace("XXX", Long.toString(DUMMY_TIME));
		System.out.println("Executing query: " + query);
		RepositoryConnection connection = store.getQueryConnection();
		TupleQuery tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, query);
		QueryRunnable queryRunnable = new QueryRunnable(tupleQuery);
		sensor.execute(queryRunnable);
		System.out.println("Result count: " + queryRunnable.getResultCount());
		connection.close();
	}

	void doEplanToVCDMTransformation(TestCase testCase, TimeSensor sensor,
	                                 RepositoryConnection source,
	                                 RepositoryConnection target) throws MalformedQueryException, RepositoryException {
		GraphQuery eplan2vcdm = source.prepareGraphQuery(
				QueryLanguage.SPARQL,
				loadQuery(testCase.getParameter("eplan2vcdmQuery").getFileValue()));
		eplan2vcdm.setIncludeInferred(true);
		TransformationRunnable transformeplan2vcdm = new TransformationRunnable(eplan2vcdm, source, target);
		System.out.println("transform eplan 2 vcdm");
		sensor.execute(transformeplan2vcdm);
		memorySensor.execute();
	}

	void doVCDMToOpmTransformation(TestCase testCase, TimeSensor sensor,
	                               RepositoryConnection source,
	                               RepositoryConnection target) throws MalformedQueryException, RepositoryException {
		GraphQuery vcdm2opm = source.prepareGraphQuery(
				QueryLanguage.SPARQL,
				loadQuery(testCase.getParameter("vcdm2opmQuery").getFileValue()));
		vcdm2opm.setIncludeInferred(true);

		TransformationRunnable transformvcdm2opm = new TransformationRunnable(vcdm2opm, source, target);
		System.out.println("transform vcdm 2 opm");
		sensor.execute(transformvcdm2opm);
		memorySensor.execute();
	}

	void doInserts(TestCase testCase, File insertsFile) throws RepositoryException, IOException, RDFParseException, MalformedQueryException {
		//temporary repository to create the changelog
		SailRepository eplanRepostiory = new SailRepository(new MemoryStore());
		eplanRepostiory.initialize();
		importInsertsFileSensor.execute(new ImportFileRunnable(eplanRepostiory.getConnection(), insertsFile));
		insertInsertsIntoStoreSensor.execute(new InsertIntoStoreRunnable(eplanRepostiory.getConnection(),
				store.getRepositoryConnection(), versioned));

		memorySensor.execute();

		SailRepository vcdmRepository = new SailRepository(new MemoryStore());
		vcdmRepository.initialize();
		doEplanToVCDMTransformation(testCase, insertsTransformEplanToVCDM, eplanRepostiory.getConnection(),
				vcdmRepository.getConnection());
		System.out.println("write transformation result to store");
		inserstInsertEplanToVCDMTranformation.execute(new InsertIntoStoreRunnable(vcdmRepository.getConnection(),
				store.getRepositoryConnection(), versioned));

		eplanRepostiory = null; //not needed anymore
		memorySensor.execute();

		SailRepository opmRepository = new SailRepository(new MemoryStore());
		opmRepository.initialize();
		doVCDMToOpmTransformation(testCase, insertTranfsformVCDMToOPM, vcdmRepository.getConnection(),
				opmRepository.getConnection());
		System.out.println("write transformation result to store");
		insertInsertVCDMToOPMTransformation.execute(new InsertIntoStoreRunnable(opmRepository.getConnection(),
				store.getRepositoryConnection(), versioned));

		memorySensor.execute();
	}

	void doUpdates(TestCase testCase, File updatesFile) throws RepositoryException, MalformedQueryException {
		SailRepository eplanRepository = new SailRepository(new MemoryStore());
		eplanRepository.initialize();

		importUpdatesFileSensor.execute(new ImportFileRunnable(eplanRepository.getConnection(), updatesFile));
		writeEplanUpdatesSensor.execute(new InsertIntoStoreRunnable(eplanRepository.getConnection(), store.getRepositoryConnection(), versioned));
		memorySensor.execute();

		SailRepository vcdmRepository = new SailRepository(new MemoryStore());
		vcdmRepository.initialize();

		doEplanToVCDMTransformation(testCase, transformUpdatesEplanToVCDMSensor, eplanRepository.getConnection(),
				vcdmRepository.getConnection());
		writeVCDMUpdatesSensor.execute(new InsertIntoStoreRunnable(vcdmRepository.getConnection(), store.getRepositoryConnection(), versioned));

		eplanRepository = null;
		memorySensor.execute();

		SailRepository opmRepository = new SailRepository(new MemoryStore());
		opmRepository.initialize();
		doVCDMToOpmTransformation(testCase, transformUpdatesVCDMToOPMSensor, vcdmRepository.getConnection(), opmRepository.getConnection());
		writeOPMUpdatesSensor.execute(new InsertIntoStoreRunnable(opmRepository.getConnection(), store.getRepositoryConnection(), versioned));
	}

	void doDeletions(TestCase testCase, File deletionsFile) throws RepositoryException, MalformedQueryException {
		SailRepository eplanRepository = new SailRepository(new MemoryStore());
		eplanRepository.initialize();
		importDeletionsFileSensor.execute(new ImportFileRunnable(eplanRepository.getConnection(), deletionsFile));
		removeEplanDeletionsSensor.execute(new DeleteFromStoreRunnable(eplanRepository.getConnection(), store.getRepositoryConnection(), versioned));

		memorySensor.execute();

		SailRepository vcdmRepository = new SailRepository(new MemoryStore());
		vcdmRepository.initialize();

		doEplanToVCDMTransformation(testCase, transformDeletionsEplanToVCDMSensor, eplanRepository.getConnection(),
				vcdmRepository.getConnection());
		System.out.println("write transformation result to sBigdataDrivertore");
		removeVCDMDeletionsSensor.execute(new DeleteFromStoreRunnable(vcdmRepository.getConnection(), store.getRepositoryConnection(), versioned));

		eplanRepository = null;     //not needed anymore
		memorySensor.execute();

		SailRepository opmRepostory = new SailRepository(new MemoryStore());
		opmRepostory.initialize();

		doVCDMToOpmTransformation(testCase, transformDeletionsVCDMToOP, vcdmRepository.getConnection(),
				opmRepostory.getConnection());
		System.out.println("write transformation result to store");
		removeOPMDeletionsSensor.execute(new DeleteFromStoreRunnable(opmRepostory.getConnection(), store.getRepositoryConnection(), versioned));

		memorySensor.execute();
	}

	@Override
	public void run(TestCase testCase) {
		DUMMY_TIME++;
		try {
			System.out.println("do inserts for commit");
			doInserts(testCase, testCase.getParameter("insertsFile").getFileValue());
			System.out.println("do updates for commit");
			doUpdates(testCase, testCase.getParameter("updatesFile").getFileValue());
			System.out.println("do deletions for commit");
			doDeletions(testCase, testCase.getParameter("deletesFile").getFileValue());

			System.out.println("run query 1");
			runQuery(loadQuery(testCase.getParameter("query1").getFileValue()), query1Sensor);
			System.out.println("run query 2");
			runQuery(loadQuery(testCase.getParameter("query2_1").getFileValue()), query2_1Sensor);
			runQuery(loadQuery(testCase.getParameter("query2_2").getFileValue()), query2_2Sensor);
			runQuery(loadQuery(testCase.getParameter("query2_3").getFileValue()), query2_3Sensor);
			System.out.println("run query 3");
			runQuery(loadQuery(testCase.getParameter("query3").getFileValue()), query3Sensor);
			System.out.println("run query 4");
			runQuery(loadQuery(testCase.getParameter("query4").getFileValue()), query4Sensor);
			System.out.println("run query 5");
			runQuery(loadQuery(testCase.getParameter("query5").getFileValue()), query5Sensor);
			System.out.println("run query 6");
			runQuery(loadQuery(testCase.getParameter("query6").getFileValue()), query6Sensor);
			System.out.println("run query 7");
			runQuery(loadQuery(testCase.getParameter("query7").getFileValue()), query7Sensor);
			System.out.println("run query 8");
			runQuery(loadQuery(testCase.getParameter("query8").getFileValue()), query8Sensor);

			ValueSensor sensor = new ValueSensor("StatementCount");
			RepositoryConnection connection = store.getRepositoryConnection();
			sensor.execute(Long.toString(connection.size()));
			sensor.close();
			/*RDFWriter writer = Rio.createWriter(RDFFormat.TURTLE, new FileOutputStream(new File(testCase.getName() + ".ttl")));
	        writer.startRDF();
            RepositoryResult<Statement> result = connection.getStatements(null, null, null, false);
            while(result.hasNext()) {
                writer.handleStatement(result.next());
            }
            writer.endRDF();*/
			connection.close();
		} catch (RepositoryException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (RDFParseException e) {
			e.printStackTrace();
		} catch (MalformedQueryException e) {
			e.printStackTrace();
		} /*catch (RDFHandlerException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }   */
	}

	private String loadQuery(File file) {
		String content = null;
		try {
			FileReader reader = new FileReader(file);
			char[] chars = new char[(int) file.length()];
			reader.read(chars);
			content = new String(chars);
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return content;
	}

	@Override
	public void finish(TestCase testCase) {
		store.closeStore();

		FileSizeSensor fileSizeSensor = new FileSizeSensor("StorageSize", store.getFileOrDirectoryList());
		fileSizeSensor.execute();
		fileSizeSensor.close();

		if (testCase.getParameter("deleteStore").getBooleanValue()) {
			store.deleteStore();
		}

		for (TimeSensor sensor : timeSensors) {
			sensor.close();
		}

		memorySensor.close();
	}
}
