package com.github.dmutti.answer;


import org.apache.commons.lang.StringUtils;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;


/**
 * Splits a line of text, filtering known stop words.
 */
public class TokenizerFn extends DoFn<String, String> {
    private static final long serialVersionUID = 1L;

    private final String separator;

    public TokenizerFn(String separator) {
        this.separator = separator;
    }

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
}
