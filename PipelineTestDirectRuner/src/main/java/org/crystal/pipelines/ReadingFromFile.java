package org.crystal.pipelines;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class ReadingFromFile {
    private static final String CSV_HEADER =
            "atmNr,country,city,address,latitude,longitude,atmAmount";

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        options.setJobName("Reading from file");
        Pipeline pipeline = Pipeline.create(options);
        long timeItTook = System.currentTimeMillis();
        PCollection<String> lines = pipeline.apply("ReadMyFile", TextIO.read().from("./src/main/resources/atmList.csv"));
        lines.apply(ParDo.of(new TotalScoreComputation.FilteredHeaderFn(CSV_HEADER)))
                .apply("printing", ParDo.of(new DoFn<String, Void>() {
                    @ProcessElement
                    public void print(ProcessContext p) {
                        System.out.println(p.element());
                    }
                }));

//        lines.apply("printing", MapElements.via(new SimpleFunction<Default.String, Void>() {
//      @DoFn.ProcessElement
//            public Void apply(DoFn.ProcessContext input){
//                System.out.println(" sout " +input);
//            }
//        }));
        pipeline.run();

        timeItTook = System.currentTimeMillis() - timeItTook;

        System.out.println(timeItTook);




        System.out.println("Runner: " + options.getRunner().getName());
        System.out.println("JobName: " + options.getJobName());
        System.out.println("OptionsID: " + options.getOptionsId());
        System.out.println("TempLocation: " + options.getTempLocation());
        System.out.println("StableUniqueNames: " + options.getStableUniqueNames());
        System.out.println("UserAgent: " + options.getUserAgent());
        System.out.println(options.getUserAgent());
        System.out.println(options.outputRuntimeOptions());
    }

    public static class FilteredHeaderFn extends DoFn<String, String> {
        private final String header;

        public FilteredHeaderFn(String header) {
            this.header = header;
        }

        @ProcessElement
        public void pel(ProcessContext processContext) {
            String row = processContext.element();

            assert row != null;
            if (!row.isEmpty() && !row.equals(this.header)) {
                processContext.output(row);
            }
        }
    }

}