package com.github.dmutti.alt;

import java.io.Serializable;
import org.apache.commons.lang.StringUtils;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
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
public class Question2Alternative extends Configured implements Tool, Serializable {

    private static final long serialVersionUID = 1L;
    public static final String separator = ",";

    @Override
    public int run(String[] args) throws Exception {
        String inputPath = args[1];
        String outputPath = args[2];

        MRPipeline pipeline = new MRPipeline(Question2Alternative.class, getConf());
        PCollection<String> lines = pipeline.readTextFile(inputPath);
        PCollection<String> words = lines.parallelDo(new DoFn<String, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public void process(String arg, Emitter<String> emitter) {
                if (arg != null && arg.length() > 1 && arg.charAt(0) == '0') {
                    emitter.emit("__zeroes__");

                } else if (arg != null && arg.length() > 1 && arg.charAt(0) == '1') {
                    String toSplit = StringUtils.substringAfter(arg, separator);
                    String[] words = toSplit.replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s+");

                    for (String word : words) {
                        if (StringUtils.isEmpty(word)) {
                            continue;
                        }
                        emitter.emit(word);
                    }
                }
            }
        }, Writables.strings());
        pipeline.write(words.count(), To.textFile(outputPath), WriteMode.APPEND);

        System.out.println("-----------------------------dot-----------------------------\n");
        System.out.println(pipeline.plan().getPlanDotFile());
        System.out.println("-----------------------------dot-----------------------------");

        PipelineResult result = pipeline.done();
        return result.succeeded() ? 0 : 1;
    }

}
