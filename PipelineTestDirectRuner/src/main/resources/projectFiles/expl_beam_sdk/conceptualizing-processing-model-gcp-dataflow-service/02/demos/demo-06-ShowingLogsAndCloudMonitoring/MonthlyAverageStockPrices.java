package org.apache.beam.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class MonthlyAverageStockPrices {

    private static final String header = "Date,Open,High,Low,Close,Adj Close,Volume";

    private static final Logger LOG = LoggerFactory.getLogger(MonthlyAverageStockPrices.class);
    
    public static void main(String[] args) {

        GcsOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(GcsOptions.class);

        processData(options);
    }

    public interface GcsOptions extends PipelineOptions {

        @Description("Path of the first file to read from")
        @Validation.Required
        String getInputFile1();

        void setInputFile1(String value);

        @Description("Path of the second file to read from")
        @Validation.Required
        String getInputFile2();

        void setInputFile2(String value);

        @Description("Path of the file to write to")
        @Validation.Required
        String getOutput();

        void setOutput(String value);
    }

    static void processData(GcsOptions options) {
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> currentYearDailyData = pipeline
                .apply("ReadStockData", TextIO.read().from(options.getInputFile1()))
                .apply("FilterHeader", ParDo.of(new FilterHeaderFn(header)));

        PCollection<String> lastYearDecData = pipeline
                .apply("ReadLastYearDecData", TextIO.read().from(options.getInputFile2()))
                .apply("FilterHeaderFromDecData", ParDo.of(new FilterHeaderFn(header)));

        final PCollectionView<Double> decAveragePrice = lastYearDecData
                .apply("ExtractCloseValues", FlatMapElements
                        .into(TypeDescriptors.doubles())
                        .via(csvRow -> Collections.singletonList(
                                Double.parseDouble(csvRow.split(",")[5]))))
                .apply("DecAvgCloseVal",
                        Combine.globally(new Average()).asSingletonView());

        PCollection<KV<Integer, Double>> averagePerMonth = currentYearDailyData
                .apply("ExtractMonthClosePrices", ParDo.of(new MakeMonthPriceKVFn()))
                .apply(Combine.perKey(new Average()));

        averagePerMonth.apply("ComparingAvgVal", ParDo.of(new DoFn<KV<Integer, Double>, String>() {

            private static final long serialVersionUID = 1L;
    
            @ProcessElement
            public void processElement(ProcessContext c)  {
                double globalAverage = c.sideInput(decAveragePrice);

                if (c.element().getValue() >= globalAverage) {

                    LOG.info("Month: " + c.element().getKey() + 
                        " has a greater average value than the previous December's average");

                    c.output(c.element().getKey() + ", " + c.element().getValue());
                } else {
                    LOG.info("Month: " + c.element().getKey() + 
                        " has a smaller average value than the previous December's average");
                }
            }
        }).withSideInputs(decAveragePrice))
                .apply("WriteResult", TextIO.write().to(options.getOutput())
                        .withoutSharding()
                        .withSuffix(".csv")
                        .withShardNameTemplate("-SSS")
                        .withHeader("Month, Average Close Value"));

        pipeline.run().waitUntilFinish();
    }

    private static class FilterHeaderFn extends DoFn<String, String> {

        private static final long serialVersionUID = 1L;

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


    private static class MakeMonthPriceKVFn extends DoFn<String, KV<Integer, Double>> {

        private static final long serialVersionUID = 1L;

        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] fields = c.element().split(",");

            DateTimeZone timeZone = DateTimeZone.forID("UTC");

            DateTime dateTime = LocalDateTime.parse(fields[0].trim(),
                    DateTimeFormat.forPattern("yyyy-MM-dd")).toDateTime(timeZone);

            c.output(KV.of(dateTime.getMonthOfYear(), Double.parseDouble(fields[5])));
        }
    }

    private static class Average implements SerializableFunction<Iterable<Double>, Double> {

        private static final long serialVersionUID = 1L;

        @Override
        public Double apply(Iterable<Double> input) {
            double sum = 0;
            int count = 0;

            for (double item : input) {
                sum += item;
                count = count + 1;
            }

            return sum / count;
        }
    }
}
