package org.jdspec.ui4j;

import java.io.File;
import org.apache.commons.io.FileUtils;
import com.ui4j.api.browser.BrowserEngine;
import com.ui4j.api.browser.BrowserFactory;
import com.ui4j.api.browser.Page;
import com.ui4j.api.browser.PageConfiguration;

public class Ui4jMain {

    public static void main(String[] args) throws Exception {
        System.setProperty("ui4j.headless", "true");
        BrowserEngine webKit = BrowserFactory.getWebKit();
        
        PageConfiguration config = new PageConfiguration();
        config.setUserAgent("Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.120 Safari/537.36");

        
        Page page = webKit.navigate("http://www.google.com.br", config);
        FileUtils.write(new File("/home/ec2-user/ui4j.html"), page.getDocument().getBody().getOuterHTML());
        page.close();
        webKit.shutdown();
    }
}
