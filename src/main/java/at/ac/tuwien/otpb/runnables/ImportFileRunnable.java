package at.ac.tuwien.otpb.runnables;

import com.hp.hpl.jena.rdf.model.Model;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

public class ImportFileRunnable implements Runnable
{
    private Model model;
    private  File file;

    public ImportFileRunnable(Model model, File file) {
        this.model = model;
        this.file = file;
    }

    @Override
    public void run() {
        try {
            FileInputStream inputStream = new FileInputStream(file);
            model.read(inputStream, "");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
