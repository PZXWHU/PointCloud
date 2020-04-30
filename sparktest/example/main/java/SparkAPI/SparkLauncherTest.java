package SparkAPI;

import org.apache.spark.deploy.worker.CommandUtils;
import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;

public class SparkLauncherTest {

    public static void main(String[] args) throws IOException {
        SparkLauncher sparkLauncher = new SparkLauncher();
        sparkLauncher.setMainClass("Test");
        sparkLauncher.setDeployMode("cluster");
        sparkLauncher.setAppResource("Potree-common.jar");
        sparkLauncher.launch();

    }

}
