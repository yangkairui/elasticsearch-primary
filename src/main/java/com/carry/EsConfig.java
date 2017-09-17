package com.carry;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by yangkr on 2017/9/17.
 */

//加入configuration,让其成为一个配置类
@Configuration
public class EsConfig {

    @Bean    //声明为一个bean
    public TransportClient client() throws UnknownHostException {
        //构建ES地址
        InetSocketTransportAddress node=new InetSocketTransportAddress(
                InetAddress.getByName("localhost"),
                9300

        );
        //设置配置
        Settings settings=Settings.builder()
                .put("cluster.name","wali")
                .build();

        TransportClient client=new PreBuiltTransportClient(settings);
        client.addTransportAddress(node);
    return client;
    }
}
