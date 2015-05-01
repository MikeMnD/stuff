package com.github.dmutti;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;
import com.github.dmutti.alt.Question2Alternative;
import com.github.dmutti.std.Question2Standard;


/**
 * @author dmutti@uolinc.com
 */
public class Runner {

    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            errorMessage();
            GenericOptionsParser.printGenericCommandUsage(System.err);
            System.exit(1);
        }

        if (StringUtils.equalsIgnoreCase("std", args[0])) {
            ToolRunner.run(new Configuration(), new Question2Standard(), args);

        } else if (StringUtils.equalsIgnoreCase("alt", args[0])) {
            ToolRunner.run(new Configuration(), new Question2Alternative(), args);

        } else {
            errorMessage();
            System.exit(1);
        }
    }

    private static void errorMessage() {
        System.err.println("Usage: hadoop jar apache-crunch-answer-1.0-SNAPSHOT-job.jar"
                        + " [std|alt] input_file output_dir");
        System.err.println();
    }
}
