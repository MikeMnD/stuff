/* Copyright (c) - UOL Inc,
 * Todos os direitos reservados
 *
 * Este arquivo e uma propriedade confidencial do Universo Online Inc.
 * Nenhuma parte do mesmo pode ser copiada, reproduzida, impressa ou
 * transmitida por qualquer meio sem autorizacao expressa e por escrito
 * de um representante legal do Universo Online Inc.
 *
 * All rights reserved
 *
 * This file is a confidential property of Universo Online Inc.
 * No part of this file may be reproduced or copied in any form or by
 * any means without written permission from an authorized person from
 * Universo Online Inc.
 */
package com.github.dmutti.answer;

import org.apache.crunch.PCollection;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.Target.WriteMode;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.To;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * apache-crunch-answer-1.0-SNAPSHOT-job.jar
 */
public class Question2 extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new Question2(), args);
    }

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: hadoop jar apache-crunch-answer-1.0-SNAPSHOT-job.jar"
                            + " [generic options] input output");
            System.err.println();
            GenericOptionsParser.printGenericCommandUsage(System.err);
            return 1;
        }

        String inputPath = args[0];
        String outputPath = args[1];

        MRPipeline pipeline = new MRPipeline(Question2.class, getConf());

        PCollection<String> lines = pipeline.readTextFile(inputPath);
        PCollection<String> zeroLines = lines.filter(new ZeroLineFilter());
        PCollection<String> oneLines = lines.filter(new ZeroLineFilter());

        PCollection<String> words = oneLines.parallelDo(new TokenizerFn(","), Writables.strings());
        PCollection<Long> zeroes = zeroLines.parallelDo(new SumFn(), Writables.longs());

        pipeline.write(zeroes.count(), To.textFile(outputPath), WriteMode.APPEND);
        pipeline.write(words.count(), To.textFile(outputPath), WriteMode.APPEND);

        System.out.println(pipeline.plan().getPlanDotFile());
        PipelineResult result = pipeline.done();
        return result.succeeded() ? 0 : 1;
    }

}
