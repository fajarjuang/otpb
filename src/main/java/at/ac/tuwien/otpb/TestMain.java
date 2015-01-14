package at.ac.tuwien.otpb;

import at.ac.tuwien.genben.TestRunner;

public class TestMain {

    public static void main(String[] args) {
        TestRunner.main(args.length == 0 ? new String[] { "sesame_config_scenario3.xml" } : args);
        // TestRunner.main(args.length == 0 ? new String[] { "virtuoso_config_scenario1.xml" } : args);
    }
}
