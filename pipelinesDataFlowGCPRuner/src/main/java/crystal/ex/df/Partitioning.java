package crystal.ex.df;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Partitioning {

    private static final String CSV_HEADER =
            "car,price,body,mileage,engV,engType,registration,year,model,drive";
    private static final Logger LOGGER = LoggerFactory.getLogger(Partitioning.class);

    public static void main(String[] args) {
        DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(DataflowPipelineOptions.class);
        options.setProject("big-data-test-369511");
        options.setRegion("us-west1");
//        options.setServiceAccount("dataflowse@big-data-test-369511.iam.gserviceaccount.com");
        options.setJobName("partitioningAndflatteningV2");
        options.setRunner(DataflowRunner.class);
        options.setGcpTempLocation("gs://d-flow-bucket1/temp");
//        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

//        PCollection<String> pCollection1 = pipeline.apply("ReadAds",
//                TextIO.read().from("gs://d-flow-bucket1/source/car_ads_1.csv"));
//        PCollection<String> pCollection2 = pipeline.apply("ReadAds",
//                TextIO.read().from("gs://d-flow-bucket1/source/car_ads_2.csv"));
//        PCollection<String> pCollection3 = pipeline.apply("ReadAds",
//                TextIO.read().from("gs://d-flow-bucket1/source/car_ads_3.csv"));
//
//        PCollectionList<String> pCollectionList = PCollectionList.of(pCollection1)
//                .and(pCollection2).and(pCollection3);
//        PCollection<String> flattenedCollection = pCollectionList.apply(Flatten.pCollections());


        PCollection<KV<String, Double>> makePriceKV = pipeline
                .apply("ReadAds", TextIO.read().from("gs://d-flow-bucket1/source/car_ads*"))
//                flattenedCollection
                .apply("FilterHeader", ParDo.of(new FilterHeaderFn(CSV_HEADER)))
                .apply("MakePriceKVFn", ParDo.of(new MakePriceKVFn()));

        PCollectionList<KV<String, Double>> priceCategories = makePriceKV
                .apply(Partition.of(4, new Partition.PartitionFn<KV<String, Double>>() {
                    @Override
                    public int partitionFor(KV<String, Double> elem, int numPartitions) {
                        if (elem.getValue() < 2000) {
                            return 0;
                        } else if (elem.getValue() < 5000) {
                            return 1;
                        } else if (elem.getValue() < 10000) {
                            return 2;
                        }
                        return 3;
                    }
                }));

        priceCategories.get(0)
                .apply("PrintToConsole pC1", ParDo.of(new DoFn<KV<String, Double>, Void>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        LOGGER.info(c.element().getKey() + ": " + c.element().getValue());
//                        System.out.println(c.element().getKey() + ": " + c.element().getValue());
                    }
                }));
        priceCategories.get(1)
                .apply("PrintToConsole pC2", ParDo.of(new DoFn<KV<String, Double>, Void>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        LOGGER.info(c.element().getKey() + ": " + c.element().getValue());

                        LOGGER.getName();
//                        System.out.println(c.element().getKey() + ": " + c.element().getValue());
                    }
                }));
                priceCategories.get(2)
                .apply("PrintToConsole pC3", ParDo.of(new DoFn<KV<String, Double>, Void>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {

                        LOGGER.info(c.element().getKey() + ": " + c.element().getValue());
                        LOGGER.getName();

//                        System.out.println(c.element().getKey() + ": " + c.element().getValue());
                    }
                }));
                priceCategories.get(3)
                .apply("PrintToConsole pC4", ParDo.of(new DoFn<KV<String, Double>, Void>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        LOGGER.info(c.element().getKey() + ": " + c.element().getValue());
                        LOGGER.getName();

//                        System.out.println(c.element().getKey() + ": " + c.element().getValue());
                    }
                }));


        pipeline.run().waitUntilFinish();
    }

    static class FilterHeaderFn extends DoFn<String, String> {

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

    private static class MakePriceKVFn extends DoFn<String, KV<String, Double>> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] fields = c.element().split(",");

            String make = fields[0];
            Double price = Double.parseDouble(fields[1]);

            c.output(KV.of(make, price));
        }
    }
}
