package com.carry;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.ExecutionException;

/**
 * Created by yangkr on 2017/9/17.
 */
@SpringBootApplication
@RestController
public class Application {
    @GetMapping("/")
    public String index(){return  "index";}

    @Autowired
    private TransportClient client;

    public static void main(String[] args) {
        SpringApplication.run(Application.class,args);
    }

    //查询
    @GetMapping("/get/book/novel")
    @ResponseBody
    public ResponseEntity get(@RequestParam(name="id") String id){
        //预执行
        GetResponse response= this.client.prepareGet("book","novel",id).get();
        if (!response.isExists()){
            return  new ResponseEntity(HttpStatus.NOT_FOUND);
        }
        return new ResponseEntity(response.getSource(), HttpStatus.OK);
    }

    //添加
    @PostMapping("/add/book/novel")
    @ResponseBody
    public ResponseEntity add(@RequestParam(name="title") String title,
                              @RequestParam(name="author") String author,
                              @RequestParam(name="word_count") String word_count ,
                              @RequestParam(name="publish_date")
                                          @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
                                      Date publish_date ){
        try {
         XContentBuilder content= XContentFactory.jsonBuilder()
                    .startObject()
                    .field("title",title)
                    .field("author",author)
                    .field("word_count",word_count)
                    .field("publish_date",publish_date.getTime());
            content.endObject();
         IndexResponse response= this.client.prepareIndex("book","novel")  //指定索引和类型
                    .setSource(content)    //设置内容
                    .get();
            return new ResponseEntity(response.getId(),HttpStatus.OK);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
    }


    @DeleteMapping("/delete/book/novel")
    @ResponseBody()
    public ResponseEntity delete(@RequestParam(name="id") String id){
      DeleteResponse response= client.prepareDelete("book","novel",id).get();
        return new ResponseEntity(HttpStatus.OK);
    }


    @PutMapping("/update/book/novel")
    @ResponseBody
    public ResponseEntity update(
            @RequestParam(name="id") String id,
                              @RequestParam(name="title") String title,
                              @RequestParam(name="author") String author,
                              @RequestParam(name="word_count") String word_count ,
                              @RequestParam(name="publish_date")
                              @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
                                      Date publish_date ){
        try {
            UpdateRequest request =new UpdateRequest("book","novel",id);

            XContentBuilder content= XContentFactory.jsonBuilder()
                    .startObject()
                    .field("title",title)
                    .field("author",author)
                    .field("word_count",word_count)
                    .field("publish_date",publish_date.getTime());
            content.endObject();
            request.doc(content);
            UpdateResponse response= this.client.update(request).get();
            return new ResponseEntity(response.getId(),HttpStatus.OK);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
    }



    //复合查询
    @PostMapping("/add/book/novel")
    @ResponseBody
    public ResponseEntity query(@RequestParam(name="title") String title,
                              @RequestParam(name="author") String author,
                              @RequestParam(name="gt_word_count") String gt_word_count ,
                              @RequestParam(name="lt_word_count") String lt_word_count ){
        try {
            BoolQueryBuilder boolQuery= QueryBuilders.boolQuery();
            boolQuery.must(QueryBuilders.matchQuery("author",author));
            boolQuery.must(QueryBuilders.matchQuery("title",title));

            RangeQueryBuilder rangeQuery=QueryBuilders.rangeQuery("word_count")
                    .from(gt_word_count);

            rangeQuery.to(lt_word_count);

            //用filter的方式将range和Bool结合起来
            boolQuery.filter(rangeQuery);

           SearchRequestBuilder builder= client.prepareSearch("book").setTypes("novel")
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setQuery(boolQuery)
                    .setFrom(0)
                    .setSize(10);
            System.out.println(builder);
            SearchResponse response=builder.get();
            for (SearchHit hit: response.getHits()) {
                System.out.println(hit.getSource());
            }
            return new ResponseEntity(HttpStatus.OK);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
    }

}
