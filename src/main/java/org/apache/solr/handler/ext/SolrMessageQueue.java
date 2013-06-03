package org.apache.solr.handler.ext;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.CloseHook;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class SolrMessageQueue extends RequestHandlerBase implements SolrCoreAware {
    private static final Logger LOGGER = LoggerFactory.getLogger(SolrMessageQueue.class);

    private class QueueUpdateWorker implements Runnable {
        public QueueUpdateWorker() {
            super();
        }

        public void run() {
            SolrQueryResponse response = performRequest();

            StringBuilder sb = new StringBuilder();
            for (Object obj : response.getValues()) {
                sb.append(obj).append('\n');
            }

            LOGGER.info(sb.toString());
        }

        private SolrQueryResponse performRequest() {
            MultiMapSolrParams solrParams = new MultiMapSolrParams(new HashMap<String, String[]>() {{
                put("command", new String[]{"delta-import"});
            }});
            SolrQueryRequestBase request = new SolrQueryRequestBase(core, solrParams) {
            };

            SolrRequestHandler requestHandler = core.getRequestHandler(pluginHandler);

            SolrQueryResponse response = new SolrQueryResponse();
            core.execute(requestHandler, request, response);

            return response;
        }
    }

    private String mqHost;

    private String queue;

    private String pluginHandler;

    private SolrCore core;

    private ExecutorService executorService;

    public SolrMessageQueue() {
        super();

        executorService = Executors.newFixedThreadPool(5);
    }

    @Override
    public void init(NamedList args) {
        super.init(args);

        mqHost = (String) this.initArgs.get("messageQueueHost");
        queue = (String) this.initArgs.get("queue");
        pluginHandler = (String) this.initArgs.get("requestHandlerName");
    }

    @Override
    public String getDescription() {
        return "SOLR MessageQueue listener";
    }

    @Override
    public String getSource() {
        return "$Source$";
    }

    @Override
    public String getVersion() {
        return "$Revision$";
    }

    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws IOException {
        rsp.add("description", "This is a simple message queueing plugin for solr.");
        rsp.add("host", mqHost);
        rsp.add("queue", queue);
        rsp.add("handler", pluginHandler);
        rsp.add("durable", Boolean.TRUE.toString());
    }

    public void inform(SolrCore core) {
        this.core = core;

        subscribe();
    }

    private void subscribe() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(mqHost);

        try {
            final Connection connection = factory.newConnection();
            final Channel channel = connection.createChannel();
            channel.queueDeclare(queue, Boolean.TRUE.booleanValue(), false, false, null);
            channel.basicConsume(queue, true, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag,
                                           Envelope envelope,
                                           AMQP.BasicProperties properties,
                                           byte[] body)
                        throws IOException {

                    executorService.execute(new QueueUpdateWorker());
                }
            });

            core.addCloseHook(new CloseHook() {
                @Override
                public void preClose(SolrCore solrCore) {
                    try {
                        channel.close();
                        connection.close();
                    } catch (IOException e) {
                        SolrException.log(LOGGER, e.getMessage(), e);
                    }
                }

                @Override
                public void postClose(SolrCore solrCore) {
                }
            });
        } catch (Exception e) {
            SolrException.log(LOGGER, e.getMessage(), e);
        }
    }
}
