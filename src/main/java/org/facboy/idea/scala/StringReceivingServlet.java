package org.facboy.idea.scala;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.spark.streaming.receiver.Receiver;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.util.Lists;

/**
 * @author Christopher Ng
 */
public class StringReceivingServlet extends HttpServlet {
    private static final Logger logger = LoggerFactory.getLogger(StringReceivingServlet.class);

    private final Receiver<String> stringReceiver;

    public StringReceivingServlet(Receiver<String> stringReceiver) {
        this.stringReceiver = stringReceiver;
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        // TODO could make this an iterator so we don't have to read the whole response into memory
        List<String> lines = Lists.newArrayList();
        try (BufferedReader reader = req.getReader()) {
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        }

        logger.info("Lines: {}", lines);
        stringReceiver.store(lines.iterator());

        resp.setStatus(HttpStatus.NO_CONTENT_204);
    }
}
