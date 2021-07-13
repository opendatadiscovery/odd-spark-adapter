package com.provectus.odd.adapters.spark;

import com.provectus.odd.api.BaseObject;
import com.provectus.odd.api.DataEntity;
import com.provectus.odd.api.EntitiesRequest;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.IOException;

@Slf4j
public class OddClient {
    private HttpClient httpClient = HttpClientBuilder.create().build();
    private String endpoint;

    public OddClient(String endpoint) {
        this.endpoint = endpoint;
        log.info("Created ODD client with endpoint: {}", endpoint);
    }

    public void submit(EntitiesRequest o) throws IOException {
        HttpPost post = new HttpPost(endpoint);
        String jsonString = Utils.toJsonString(o);
        post.setEntity(new StringEntity(jsonString));
        post.setHeader("Content-type", "application/json");
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    log.info("Submitting: {}", jsonString);
                    HttpResponse response = httpClient.execute(post);
                    log.info("Response: {}", response);
                } catch (IOException e) {
                    log.error("Error sending request", e);
                }
            }
        }).start();
        log.info("Request sening initiated");
    }
}
