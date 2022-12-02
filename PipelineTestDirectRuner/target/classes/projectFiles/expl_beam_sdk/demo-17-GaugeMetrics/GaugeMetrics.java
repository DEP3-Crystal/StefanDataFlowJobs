import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.*;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;

public class GaugeMetrics {

    private static final String CSV_HEADER = 
            "car,price,body,mileage,engV,engType,registration,year,model,drive";

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("ReadAds", TextIO.read().from("src/main/resources/source/car_ads*.csv"))
                .apply("FilterHeader", ParDo.of(new FilterHeaderFn(CSV_HEADER)))
                .apply("FilterSedanHatchback", ParDo.of(new FilterSedanHatchbackFn()))
                .apply("FilterPrice", ParDo.of(new FilterPriceFn(2000)))
                .apply("PrintToConsole", ParDo.of(new DoFn<String, Void>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        System.out.println(c.element());
                    }
                }));

        PipelineResult pipelineResult = pipeline.run();
        pipelineResult.waitUntilFinish();

        queryAndPrintMetricResults(pipelineResult, "CarPrices", "gauge");
        queryAndPrintMetricResults(pipelineResult, "SedanHatchback", "gauge");
        queryAndPrintMetricResults(pipelineResult, "PriceThreshold", "gauge");
    }

    private static void queryAndPrintMetricResults(
            PipelineResult pipelineResult, String namespace, String name) {

        MetricQueryResults metrics = pipelineResult.metrics().queryMetrics(MetricsFilter.builder()
                .addNameFilter(MetricNameFilter.named(namespace, name)).build());

        for (MetricResult<GaugeResult> distribution: metrics.getGauges()) {
            System.out.println("*****" + distribution.getName() +
                    ": " + distribution.getCommitted());
        }
    }

    private static class FilterHeaderFn extends DoFn<String, String> {

        private final String header;
        private final Gauge carPriceGauge = Metrics.gauge(
                "CarPrices", "gauge");

        public FilterHeaderFn(String header) {
            this.header = header;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String row = c.element();

            if (!row.isEmpty() && !row.equals(this.header)) {

                String[] fields = row.split(",");
                long price = Math.round(Double.parseDouble(fields[1]));

                carPriceGauge.set(price);
                c.output(row);
            }
        }
    }

    private static class FilterSedanHatchbackFn extends DoFn<String, String> {

        private final Gauge sedanHatchbackPriceGauge = Metrics.gauge(
                "SedanHatchback", "gauge");

        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] fields = c.element().split(",");

            String body = fields[2];

            if (body.equals("sedan") || body.equals("hatch")) {
                long price = Math.round(Double.parseDouble(fields[1]));

                sedanHatchbackPriceGauge.set(price);

                c.output(c.element());
            }
        }
    }

    private static class FilterPriceFn extends DoFn<String, String> {

        private Double priceThreshold = 0.0;

        private final Gauge thresholdPriceGauge = Metrics.gauge(
                "PriceThreshold", "gauge");

        public FilterPriceFn(double priceThreshold) {
            this.priceThreshold = priceThreshold;
        }

        @ProcessElement
        public void processElement(@Element String line, OutputReceiver<String> out) {
            String[] fields = line.split(",");

            double price = Double.parseDouble(fields[1]);

            if (price != 0 && price < priceThreshold) {
                thresholdPriceGauge.set(Math.round(price));
                out.output(line);
            }
        }
    }
}
