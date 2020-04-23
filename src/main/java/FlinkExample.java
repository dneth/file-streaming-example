import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.aws.options.S3Options;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FlinkExample {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkExample.class);

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
            LOGGER.info("combining");
            return inputs.iterator().next();
        }
    }

    public static class CombineFn extends DoFn<KV<String, String>, String> {
        private static final Logger LOGGER = LoggerFactory.getLogger(CombineFn.class);

        @ProcessElement
        public void processElement(@Element KV<String, String> kv, OutputReceiver<String> receiver, ProcessContext ctx,
                                   PaneInfo pane, IntervalWindow window) {
            LOGGER.info("max: " + window.maxTimestamp());
            LOGGER.info("end: " + window.end());
            LOGGER.info("current: " + ctx.timestamp());
            LOGGER.info("start: " + window.start());
            LOGGER.info("range seconds: " + (window.end().getMillis() - window.start().getMillis())/1000);

            receiver.output(kv.getValue());
        }
    }

    public interface ProcessingOptions extends S3Options {

        @Description("The directory to use for reads/writes")
        @Validation.Required
        String getDirectory();

        void setDirectory(String value);
    }

    public static void main(final String[] args) throws Exception {
        LOGGER.info("starting");

        final ProcessingOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(ProcessingOptions.class);
        final String inputDir = options.getDirectory() + "/input/";
        final String outputFile = options.getDirectory() + "/output/";

        final TextInputFormat inputFormat = new TextInputFormat(new org.apache.flink.core.fs.Path(inputDir));


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataStreamSink<String> input = env.readFile(inputFormat, inputDir, FileProcessingMode.PROCESS_CONTINUOUSLY, 1000)
                .name("read")
                .keyBy(new KeySelector<String, String>() {

            @Override
            public String getKey(String value) throws Exception {
                return "key";
            }
        }).timeWindowAll(Time.seconds(5))
                .reduce(new ReduceFunction<String>() {
                    @Override
                    public String reduce(String value1, String value2) throws Exception {
                        return value1;
                    }
                })
                .writeAsText(outputFile);

        env.execute("native flink example");
    }
}
