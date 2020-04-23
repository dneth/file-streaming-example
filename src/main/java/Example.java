import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.aws.options.S3Options;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.beam.sdk.values.TypeDescriptors.strings;
import static org.apache.beam.sdk.values.TypeDescriptors.voids;


public class Example {
    private static final Logger LOGGER = LoggerFactory.getLogger(Example.class);

    public static class SplitFn extends DoFn<String, KV<String, String>> {
        private static final Logger LOGGER = LoggerFactory.getLogger(SplitFn.class);

        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<KV<String, String>> receiver, ProcessContext ctx,
                                   PaneInfo pane, IntervalWindow window) {
            LOGGER.info("max: " + window.maxTimestamp());
            LOGGER.info("end: " + window.end());
            LOGGER.info("current: " + ctx.timestamp());
            LOGGER.info("start: " + window.start());
            LOGGER.info("range seconds: " + (window.end().getMillis() - window.start().getMillis())/1000);

            receiver.output(KV.of("key", element));
        }
    }

    public static class CombinePerKeyFn implements SerializableFunction<Iterable<String>, String> {
        private static final Logger LOGGER = LoggerFactory.getLogger(CombinePerKeyFn.class);

        @Override
        public String apply(Iterable<String> inputs) {
            LOGGER.info("combining " + inputs);
            return inputs.iterator().next();
        }
    }

    public interface ProcessingOptions extends PipelineOptions {

//        @Description("The directory to use for reads/writes")
//        @Validation.Required
//        String getDirectory();
//
//        void setDirectory(String value);

        @Description("whether to use a splittable source or not")
        @Default.Boolean(true)
        boolean getSplittable();

        void setSplittable(boolean isSplittable);
    }

    public static void main(final String[] args) {
        final ProcessingOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(ProcessingOptions.class);
        final Pipeline pipeline = Pipeline.create(options);

        PCollection<String> initial;
        if(options.getSplittable()) {
//            pipeline.apply(FileIO.match().filepattern(options.getDirectory() + "/input/*").continuously(Duration.standardSeconds(100), Watch.Growth.never()))
//                    .apply("Read", FileIO.readMatches())
//                    .apply(TextIO.readFiles())


            initial = pipeline
                    .apply(Create.of(1))
                    .apply(Watch.growthOf(
                            new Watch.Growth.PollFn<Integer, String>() {
                                private static final long serialVersionUID = 1L;
                                @Override
                                public Watch.Growth.PollResult<String> apply(Integer element, Context c) throws Exception {
                                    Instant now = Instant.now();
                                    return Watch.Growth.PollResult.incomplete(
                                            now,
                                            Collections.singletonList(
                                                    "test" + ThreadLocalRandom.current().nextInt(10)))
                                            .withWatermark(now);
                                }
                            })
                            .withTerminationPerInput(Watch.Growth.never())
                            .withPollInterval(Duration.standardSeconds(2)))
                    .apply(Values.create());
        } else {
            initial = pipeline
                    .apply(GenerateSequence.from(0).withRate(1, Duration.standardSeconds(2)))
                    .apply(MapElements.into(strings()).via(i -> "test" + ThreadLocalRandom.current().nextInt(10)));
        }

        initial
                .apply(Window.<String>into(FixedWindows.of(Duration.standardSeconds(6))))
                .apply(ParDo.of(new SplitFn()))
                .apply(Combine.perKey(new CombinePerKeyFn()))
                .apply(
                        MapElements.into(voids())
                                .via(
                                        s -> {
                                            System.out.println(s.getValue());
                                            return null;
                                        }));

        pipeline.run(options);
    }
}
