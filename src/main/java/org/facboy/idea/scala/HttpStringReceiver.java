package org.facboy.idea.scala;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.eclipse.jetty.plus.servlet.ServletHandler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.servlet.ServletHolder;

import com.google.inject.Inject;

/**
 * @author Christopher Ng
 */
public class HttpStringReceiver extends Receiver<String> {
    private static Server server;
    private static ServletHandler servletHandler;

    private final String servletPath;

    @Inject
    public HttpStringReceiver(StorageLevel storageLevel, String servletPath) {
        super(storageLevel);
        this.servletPath = servletPath;
    }

    @Override
    public void onStart() {
        ServletHandler h = getServletHandler();
        synchronized (h) {
            h.addServletWithMapping(new ServletHolder(new StringReceivingServlet(this)), servletPath);
        }
    }

    @Override
    public void onStop() {
        try {
            Server s = getServer();
            if (s != null) {
                s.stop();
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static synchronized ServletHandler getServletHandler() {
        if (server == null) {
            Server localServer = new Server();

            SelectChannelConnector selectChannelConnector = new SelectChannelConnector();
            selectChannelConnector.setPort(9991);
            localServer.addConnector(selectChannelConnector);

            ServletHandler localServletHandler = new ServletHandler();
            localServer.setHandler(localServletHandler);

            try {
                localServer.start();
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            server = localServer;
            servletHandler = localServletHandler;
        }
        return servletHandler;
    }

    private static synchronized Server getServer() {
        return server;
    }
}
