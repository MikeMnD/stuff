package com.github.dmutti.std;

import java.io.Serializable;
import org.apache.commons.lang.StringUtils;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.FilterFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.Target.WriteMode;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.To;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

/**
 * apache-crunch-answer-1.0-SNAPSHOT-job.jar
 */
public class Question2Standard extends Configured implements Tool, Serializable {

    private static final long serialVersionUID = 1L;
    private static final String separator = ",";

    @Override
    public int run(String[] args) throws Exception {
        String inputPath = args[1];
        String outputPath = args[2];

        MRPipeline pipeline = new MRPipeline(Question2Standard.class, getConf());

        PCollection<String> lines = pipeline.readTextFile(inputPath);
        PCollection<String> zeroLines = lines.filter(new FilterFn<String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public boolean accept(String arg) {
                return arg != null && arg.length() > 0 && arg.charAt(0) == '0';
            }
        });
        PCollection<String> oneLines = lines.filter(new FilterFn<String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public boolean accept(String arg) {
                return arg != null && arg.length() > 0 && arg.charAt(0) == '1';
            }
        });

        PCollection<String> words = oneLines.parallelDo(new DoFn<String, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public void process(String line, Emitter<String> emitter) {
                String toSplit = StringUtils.substringAfter(line, separator);
                String[] words = toSplit.replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s+");

                for (String word : words) {
                    if (StringUtils.isEmpty(word)) {
                        continue;
                    }
                    emitter.emit(word);
                }
            }
        }, Writables.strings());
        PCollection<Long> zeroes = zeroLines.parallelDo(new DoFn<String, Long>() {
            private static final long serialVersionUID = 1L;

            @Override
            public void process(String arg, Emitter<Long> emitter) {
                emitter.emit(1L);
            }
        }, Writables.longs());

        pipeline.write(zeroes.count(), To.textFile(outputPath), WriteMode.APPEND);
        pipeline.write(words.count(), To.textFile(outputPath), WriteMode.APPEND);

        System.out.println("-----------------------------dot-----------------------------");
        System.out.println(pipeline.plan().getPlanDotFile());
        System.out.println("-----------------------------dot-----------------------------");

        PipelineResult result = pipeline.done();
        return result.succeeded() ? 0 : 1;
    }

}
