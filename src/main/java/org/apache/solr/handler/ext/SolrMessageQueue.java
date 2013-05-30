package org.apache.solr.handler.ext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.plugin.SolrCoreAware;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

public class SolrMessageQueue extends RequestHandlerBase implements SolrCoreAware {
    protected String mqHost;
    protected ConnectionFactory factory;
    protected String queue;
    protected String pluginHandler;
    protected Boolean durable = Boolean.TRUE;
    protected SolrCore core;

    public SolrMessageQueue() {
        super();
    }

    @Override
    public void init(NamedList args) {
        super.init(args);

        mqHost = (String) this.initArgs.get("messageQueueHost");
        queue = (String) this.initArgs.get("queue");
        pluginHandler = (String) this.initArgs.get("requestHandlerName");
        factory = new ConnectionFactory();
        factory.setHost(mqHost);

        QueueListener listener = new QueueListener();
        listener.start();
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
        rsp.add("durable", durable.toString());
    }

    public SolrQueryResponse performRequest() {
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

    public void inform(SolrCore core) {
        this.core = core;
    }

    private class QueueListener extends Thread {
        public void run() {
            Connection connection;
            try {
                connection = factory.newConnection();
                Channel channel = connection.createChannel();
                channel.queueDeclare(queue, durable.booleanValue(), false, false, null);
                QueueingConsumer consumer = new QueueingConsumer(channel);
                channel.basicConsume(queue, true, consumer);

                while (true) {
                    QueueingConsumer.Delivery delivery = consumer.nextDelivery();
                    QueueUpdateWorker worker = new QueueUpdateWorker(delivery);
                    worker.start();
                }
            } catch (Exception e) {
                SolrException.log(SolrCore.log, e.getMessage(), e);
            }
        }
    }

    private class QueueUpdateWorker extends Thread {
        QueueingConsumer.Delivery delivery;

        public QueueUpdateWorker(QueueingConsumer.Delivery delivery) {
            super();
            this.delivery = delivery;
        }

        public void run() {
            SolrQueryResponse response = performRequest();

            StringBuilder sb = new StringBuilder();
            for (Object obj : response.getValues()) {
                sb.append(obj);
            }
            System.out.println(sb);
            SolrCore.log.warn(sb.toString());
        }
    }
}
