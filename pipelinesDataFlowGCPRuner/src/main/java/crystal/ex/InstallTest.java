package crystal.ex;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.values.PCollection;

public class InstallTest {

    public static void main(String[] args) {

        Pipeline pipeline = Pipeline.create();
        PCollection<String> outputList = pipeline.apply(TextIO.read().from("src/main/resources/Doc.txt"));
        outputList.apply(TextIO.write().to("src/main/resources/output.csv").withSuffix(".csv").withHeader("header").withFooter("FOOTER").withWindowedWrites());

        pipeline.run();

    }
}

