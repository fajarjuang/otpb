package at.ac.tuwien.otpb;

import at.ac.tuwien.genben.TestDriver;
import at.ac.tuwien.genben.sensors.FileSizeSensor;
import at.ac.tuwien.genben.sensors.MemorySensor;
import at.ac.tuwien.genben.sensors.TimeSensor;
import at.ac.tuwien.genben.sensors.ValueSensor;
import at.ac.tuwien.genben.xml.TestCase;
import at.ac.tuwien.otpb.runnables.*;
import at.ac.tuwien.otpb.store.Store;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFFormat;

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
        String storeClassName = testCase.getParameter("store").getName();
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

    void runQuery(String query, TimeSensor sensor) {
        query = query.replace("XXX", Long.toString(DUMMY_TIME));
        System.out.println("Executing query: " + query);
        Model model = store.getQueryConnection();
        Query q = QueryFactory.create(query);
        QueryExecution tupleQuery = QueryExecutionFactory.create(q, model);
        QueryRunnable queryRunnable = new QueryRunnable(tupleQuery);
        sensor.execute(queryRunnable);
        System.out.println("Result count: " + queryRunnable.getResultCount());
    }

    void doEplanToVCDMTransformation(TestCase testCase, TimeSensor sensor,
                                     Model source,
                                     Model target) {
        Query q = QueryFactory.create(loadQuery(testCase.getParameter("eplan2vcdmQuery").getFileValue()));
        QueryExecution eplan2vcdm = QueryExecutionFactory.create(q, source);
        TransformationRunnable transformeplan2vcdm = new TransformationRunnable(eplan2vcdm, source, target);
        System.out.println("transform eplan 2 vcdm");
        sensor.execute(transformeplan2vcdm);
        memorySensor.execute();
    }

    void doVCDMToOpmTransformation(TestCase testCase, TimeSensor sensor,
                                   Model source,
                                   Model target) {
        Query q = QueryFactory.create(loadQuery(testCase.getParameter("vcdm2opmQuery").getFileValue()));
        QueryExecution query = QueryExecutionFactory.create(q, source);
        TransformationRunnable transformvcdm2opm = new TransformationRunnable(query, source, target);
        System.out.println("transform vcdm 2 opm");
        sensor.execute(transformvcdm2opm);
        memorySensor.execute();
    }

    void doInserts(TestCase testCase, File insertsFile) {
        //temporary repository to create the changelog
        Model eplanRepostiory = ModelFactory.createDefaultModel();
        importInsertsFileSensor.execute(new ImportFileRunnable(eplanRepostiory, insertsFile));
        insertInsertsIntoStoreSensor.execute(new InsertIntoStoreRunnable(eplanRepostiory,
                store.getRepositoryConnection(), versioned));

        memorySensor.execute();

        Model vcdmRepository = ModelFactory.createDefaultModel();
        doEplanToVCDMTransformation(testCase, insertsTransformEplanToVCDM, eplanRepostiory,
                vcdmRepository);
        System.out.println("write transformation result to store");
        inserstInsertEplanToVCDMTranformation.execute(new InsertIntoStoreRunnable(vcdmRepository,
                store.getRepositoryConnection(), versioned));

        eplanRepostiory = null; //not needed anymore
        memorySensor.execute();

        Model opmRepository = ModelFactory.createDefaultModel();
        doVCDMToOpmTransformation(testCase, insertTranfsformVCDMToOPM, vcdmRepository,
                opmRepository);
        System.out.println("write transformation result to store");
        insertInsertVCDMToOPMTransformation.execute(new InsertIntoStoreRunnable(opmRepository,
                store.getRepositoryConnection(), versioned));

        memorySensor.execute();
    }

    void doUpdates(TestCase testCase, File updatesFile) {
        Model eplanRepository = ModelFactory.createDefaultModel();

        importUpdatesFileSensor.execute(new ImportFileRunnable(eplanRepository, updatesFile));
        writeEplanUpdatesSensor.execute(new InsertIntoStoreRunnable(eplanRepository, store.getRepositoryConnection(), versioned));
        memorySensor.execute();

        Model vcdmRepository = ModelFactory.createDefaultModel();

        doEplanToVCDMTransformation(testCase, transformUpdatesEplanToVCDMSensor, eplanRepository,
                vcdmRepository);
        writeVCDMUpdatesSensor.execute(new InsertIntoStoreRunnable(vcdmRepository, store.getRepositoryConnection(), versioned));

        eplanRepository = null;
        memorySensor.execute();

        Model opmRepository = ModelFactory.createDefaultModel();
        doVCDMToOpmTransformation(testCase, transformUpdatesVCDMToOPMSensor, vcdmRepository, opmRepository);
        writeOPMUpdatesSensor.execute(new InsertIntoStoreRunnable(opmRepository, store.getRepositoryConnection(), versioned));
    }

    void doDeletions(TestCase testCase, File deletionsFile) {
        Model eplanRepository = ModelFactory.createDefaultModel();
        importDeletionsFileSensor.execute(new ImportFileRunnable(eplanRepository, deletionsFile));
        removeEplanDeletionsSensor.execute(new DeleteFromStoreRunnable(eplanRepository, store.getRepositoryConnection(), versioned));

        memorySensor.execute();

        Model vcdmRepository = ModelFactory.createDefaultModel();

        doEplanToVCDMTransformation(testCase, transformDeletionsEplanToVCDMSensor, eplanRepository,
                vcdmRepository);
        System.out.println("write transformation result to sBigdataDrivertore");
        removeVCDMDeletionsSensor.execute(new DeleteFromStoreRunnable(vcdmRepository, store.getRepositoryConnection(), versioned));

        eplanRepository = null;     //not needed anymore
        memorySensor.execute();

        Model opmRepostory = ModelFactory.createDefaultModel();

        doVCDMToOpmTransformation(testCase, transformDeletionsVCDMToOP, vcdmRepository,
                opmRepostory);
        System.out.println("write transformation result to store");
        removeOPMDeletionsSensor.execute(new DeleteFromStoreRunnable(opmRepostory, store.getRepositoryConnection(), versioned));

        memorySensor.execute();
    }

    @Override
    public void run(TestCase testCase) {
        DUMMY_TIME++;
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
        Model model = store.getRepositoryConnection();
        sensor.execute(Long.toString(model.size()));
        sensor.close();
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
