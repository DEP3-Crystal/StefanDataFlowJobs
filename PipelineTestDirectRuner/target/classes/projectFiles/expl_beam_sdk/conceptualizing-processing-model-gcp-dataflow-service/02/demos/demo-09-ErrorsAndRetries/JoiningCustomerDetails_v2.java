package org.apache.beam.examples;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.json.JSONObject;

import java.util.ArrayList;


public class JoiningCustomerDetails {

    private static final String CSV_INFO_HEADER = "CustomerID,Gender,Age,Annual_Income";
    private static final String CSV_SCORE_HEADER = "CustomerID,Spending Score";

    public interface GcsOptions extends PipelineOptions {
        @Description("Path of the income file to read from")
        @Validation.Required
        String getInputFile1();
        void setInputFile1(String value);

        @Description("Path of the spending score file to read from")
        @Validation.Required
        String getInputFile2();
        void setInputFile2(String value);

        @Description("Table name")
        @Validation.Required
        String getTableName();
        void setTableName(String value);
    }


    public static void main(String[] args) {

        GcsOptions options = PipelineOptionsFactory.fromArgs(args)
            .withValidation().as(GcsOptions.class);

        performJoin(options);
    }

    static void performJoin(GcsOptions options) {

        Pipeline pipeline = Pipeline.create(options);

        PCollection<KV<String, Integer>> customersIncome = pipeline
                .apply("ReadIncome", TextIO.read().from(options.getInputFile1()))
                .apply("FilterInfoHeader", ParDo.of(new FilterHeaderFn(CSV_INFO_HEADER)))
                .apply("IdIncomeKV", ParDo.of(new IdIncomeKVFn()));

        PCollection<KV<String, Integer>> customersScore = pipeline
                .apply("ReadSpendingScore", TextIO.read().from(options.getInputFile2()))
                .apply("FilterScoreHeader", ParDo.of(new FilterHeaderFn(CSV_SCORE_HEADER)))
                .apply("IdScoreKV", ParDo.of(new IdScoreKVFn()));

        final TupleTag<Integer> incomeTag = new TupleTag<>();
        final TupleTag<Integer> scoreTag = new TupleTag<>();

        PCollection<KV<String, CoGbkResult>> joined = KeyedPCollectionTuple
                .of(incomeTag, customersIncome)
                .and(scoreTag, customersScore)
                .apply(CoGroupByKey.create());

        joined.apply("Joining", ParDo.of(
                new DoFn<KV<String, CoGbkResult>, TableRow>() {
                    
                    @ProcessElement
                    public void processElement(
                            @Element KV<String, CoGbkResult> element,
                            OutputReceiver<TableRow> out) {

                        String id = element.getKey();

                        Integer income = element.getValue().getOnly(incomeTag);
                        Integer spendingScore = element.getValue().getOnly(scoreTag);

                        out.output(new TableRow()
                                .set(FormatSchema.CUSTOMER_ID, id)
                                .set(FormatSchema.CUSTOMER_INCOME, income)
                                .set(FormatSchema.CUSTOMER_SCORE, spendingScore));
                    }
                })).apply("WriteDataToBigQuery", BigQueryIO.writeTableRows().to("loony-dataflow-review:results." + options.getTableName())
                .withSchema(FormatSchema.getSchema()));

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

    private static class IdIncomeKVFn extends DoFn<String, KV<String, Integer>> {

        @ProcessElement
        public void processElement(
                @Element String element,
                OutputReceiver<KV<String, Integer>> out) {
            String[] fields = element.split(",");

            String id = fields[0];
            int income = Integer.parseInt(fields[3]);

            out.output(KV.of(id, income));
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

    public static class FormatSchema extends DoFn<String, TableRow> {

        public static final String CUSTOMER_ID = "id";
        public static final String CUSTOMER_INCOME = "income";
        public static final String CUSTOMER_SCORE = "score";

        @ProcessElement
        public void processElement(DoFn<String, TableRow>.ProcessContext c) {
            String input = c.element();

            JSONObject obj = new JSONObject(input);
            c.output(new TableRow()
                    .set(CUSTOMER_ID, obj.getInt(CUSTOMER_ID))
                    .set(CUSTOMER_INCOME, obj.getInt(CUSTOMER_INCOME))
                    .set(CUSTOMER_SCORE, obj.getInt(CUSTOMER_SCORE))
            );
        }

        public static TableSchema getSchema() {
            return new TableSchema().setFields(new ArrayList<TableFieldSchema>() {
                {
                    add(new TableFieldSchema().setName(CUSTOMER_ID).setType("INTEGER"));
                    add(new TableFieldSchema().setName(CUSTOMER_INCOME).setType("INTEGER"));
                    add(new TableFieldSchema().setName(CUSTOMER_SCORE).setType("INTEGER"));
                }
            });
        }
    }
}

