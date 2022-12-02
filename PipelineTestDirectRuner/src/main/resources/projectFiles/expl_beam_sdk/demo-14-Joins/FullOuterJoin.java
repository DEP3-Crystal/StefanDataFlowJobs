import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.apache.beam.sdk.extensions.joinlibrary.Join;

public class FullOuterJoin {

    private static final String CSV_INFO_HEADER = "CustomerID,Gender,Age,Annual_Income";
    private static final String CSV_SCORE_HEADER = "CustomerID,Spending Score";

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<KV<String, String>> customersGender = pipeline
                .apply(TextIO.read().from("src/main/resources/source/mall_customers_info.csv"))
                .apply("FilterInfoHeader", ParDo.of(new FilterHeaderFn(CSV_INFO_HEADER)))
                .apply("IdGenderKV", ParDo.of(new IdGenderKVFn()));

        PCollection<KV<String, Integer>> customersScore = pipeline
                .apply(TextIO.read().from("src/main/resources/source/mall_customers_score.csv"))
                .apply("FilterScoreHeader", ParDo.of(new FilterHeaderFn(CSV_SCORE_HEADER)))
                .apply("IdScoreKV", ParDo.of(new IdScoreKVFn()));

        PCollection<KV<String, KV<String, Integer>>> joinedDatasets = Join.fullOuterJoin(
                customersGender, customersScore, "Unavailable", -1);

        joinedDatasets.apply(MapElements.via(
                new SimpleFunction<KV<String, KV<String, Integer>>, Void>() {

            @Override
            public Void apply(KV<String, KV<String, Integer>> input) {
                System.out.println(input.getKey() + ", " +
                        input.getValue().getKey() + ", " + input.getValue().getValue());
                return null;
            }

        }));

        pipeline.run().waitUntilFinish();

    }

    private static class FilterHeaderFn extends DoFn<String, String> {

        private final String header;

        public FilterHeaderFn(String header) {
            this.header = header;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String row = c.element();

            if (!row.isEmpty() && !row.equals(this.header)) {
                c.output(row);
            }
        }
    }

    private static class IdGenderKVFn extends DoFn<String, KV<String, String>> {

        @ProcessElement
        public void processElement(
                @Element String element,
                OutputReceiver<KV<String, String>> out) {
            String[] fields = element.split(",");

            out.output(KV.of(fields[0], fields[1]));
        }
    }

    private static class IdScoreKVFn extends DoFn<String, KV<String, Integer>> {

        @ProcessElement
        public void processElement(
                @Element String element,
                OutputReceiver<KV<String, Integer>> out) {
            String[] fields = element.split(",");

            String id = fields[0];
            int score = Integer.parseInt(fields[1]);

            out.output(KV.of(id, score));
        }
    }

}
