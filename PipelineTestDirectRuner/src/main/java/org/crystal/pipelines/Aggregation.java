package org.crystal.pipelines;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.joda.time.Duration;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

public class Aggregation {
    static Logger logger = Logger.getLogger(Aggregation.class.getName());
    static long time=System.currentTimeMillis();

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        List<Double> list = Arrays.asList(1555.5, 1220.5, 1394.2, 1404.6);

        pipeline.apply(Create.of(list))
                .apply(Mean.<Double>globally())
                .apply(MapElements.via(new SimpleFunction<Double, Void>() {
                    @Override
                    public Void apply(Double input) {
                        logger.log(Level.INFO,"Average stock price ");
                        return null;
                    }
                }));

        pipeline.run();

        Random random=new Random();
        int e= random.nextInt(100);
        logger.info(String.valueOf(e));
        pipeline.apply("Ticker", GenerateSequence.from(0).withRate(1, Duration.standardSeconds(5)))
                .apply(ParDo.of(new DoFn<Long, Long>() {
                    @ProcessElement
                    public void processElement(@Element Long tick, OutputReceiver<Long> out) {
                        ZonedDateTime currentInstant = Instant.now().atZone(ZoneId.of("Asia/Jakarta"));
                        logger.warning("-" + tick + "-" + currentInstant);
                        logger.log(Level.INFO,"Call date:  "+ new Date());
                        time=System.currentTimeMillis()-time;
                        logger.log(Level.INFO,"Took:  "+ time);
                                          }
                }));
        pipeline.run().waitUntilFinish();

    }
    //every 15 sec write a line in the screen


}
