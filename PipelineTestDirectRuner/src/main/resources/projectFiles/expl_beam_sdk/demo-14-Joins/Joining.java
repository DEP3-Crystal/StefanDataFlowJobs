import org.apache.beam.repackaged.core.org.apache.commons.lang3.SerializationUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;

import java.io.IOException;
import java.io.StringReader;


public class Joining {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<UserSession> userSessions = pipeline
                .apply("ReadUserSessions",
                        TextIO.read().from("src/main/resources/source/tags.csv"))
                .apply("ParseUserSessions",
                        ParDo.of(new ParseUserSessions()))
                .apply("Timestamps",
                        WithTimestamps.of(UserSession::getTimestamp))
                .apply("SetTimeStampCombinerForTags",
                    Window.<UserSession>into(new GlobalWindows())
                        .withTimestampCombiner(TimestampCombiner.EARLIEST));

        PCollection<KV<String, UserSession>> keyedUserSessions = userSessions
                .apply("MapByMovieId", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptor.of(UserSession.class)))
                        .via(us -> KV.of(us.getMovieId(), us)));

        PCollection<KV<String, String>> movieTitles = pipeline
                .apply("ReadMovieTitles",
                        TextIO.read().from("src/main/resources/source/movies.csv"))
                .apply("ParseMovieTitles",
                        ParDo.of(new ParseMovieTitles()))
                .apply("SetTimeStampCombinerForTitles",
                        Window.<KV<String,String>>into(new  GlobalWindows())
                                .withTimestampCombiner(TimestampCombiner.EARLIEST));

        TupleTag<UserSession> userSessionsTag = new TupleTag<>();
        TupleTag<String> titleTag = new TupleTag<>();

        PCollection<KV<String, CoGbkResult>> moviesAndTitles = KeyedPCollectionTuple
                .of(userSessionsTag, keyedUserSessions)
                .and(titleTag, movieTitles)
                .apply(CoGroupByKey.create());

        PCollection<UserSession> movieTagsWithTitle = moviesAndTitles
                .apply("AddTitleToMovieTag",
                        ParDo.of(new DoFn<KV<String, CoGbkResult>, UserSession>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                String title = c.element().getValue().getOnly(titleTag);
                                if (title == null) {
                                    return;
                                }

                                for(UserSession us : c.element().getValue().getAll(userSessionsTag)){
                                    UserSession out = SerializationUtils.clone(us);

                                    out.setMovieTitle(title);
                                    c.output(out);
                                }
                            }
                        }));

        movieTagsWithTitle
                .apply("Timestamps",
                        WithTimestamps.of(UserSession::getTimestamp))
                .apply("Window", Window.into(FixedWindows.of(Duration.standardDays(365))))
                .apply("ToStrings", MapElements
                        .into(TypeDescriptors.strings())
                        .via(us -> us.asCSVRow(",")))
                .apply("WriteToFile", TextIO
                        .write()
                        .to("src/main/resources/sink/movies").withSuffix(".csv")
                        .withHeader(UserSession.getCSVHeaders())
                        .withNumShards(1)
                        .withWindowedWrites());

        pipeline.run().waitUntilFinish();

    }

    private static class ParseUserSessions extends DoFn<String, UserSession> {
        public static final String[] FILE_HEADER_MAPPING = {
                "sessionId","userId","movieId", "tag","timestamp"
        };

        @ProcessElement
        public void processElement(ProcessContext c) throws IOException {
            final CSVParser parser = new CSVParser(new StringReader(c.element()), CSVFormat.DEFAULT
                    .withDelimiter(',')
                    .withHeader(FILE_HEADER_MAPPING));

            CSVRecord record = parser.getRecords().get(0);

            // Skip over the header row
            if (record.get("timestamp").contains("timestamp") ){
                return;
            }

            DateTimeZone timeZone = DateTimeZone.forID("UTC");

            DateTime startedWatching = LocalDateTime.parse(record.get("timestamp").trim(),
                    DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")).toDateTime(timeZone);

            UserSession userSession = new UserSession();
            userSession.setUserId(record.get("userId"));
            userSession.setSessionId(Integer.valueOf(record.get("sessionId")));
            userSession.setMovieId(record.get("movieId").trim());
            userSession.setTag(record.get("tag").trim());
            userSession.setTimestamp(startedWatching.toInstant());

            c.output(userSession);
        }
    }

    public static class ParseMovieTitles extends DoFn<String,KV<String,String>> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            final String[] row = c.element().split(",");

            if (row[0].contains("VideoId")){
                return;
            }

            c.output(KV.of(row[0].trim(), row[1].trim()));
        }
    }

}
