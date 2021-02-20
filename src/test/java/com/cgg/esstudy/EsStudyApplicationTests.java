package com.cgg.esstudy;

import com.alibaba.fastjson.JSON;
import com.cgg.esstudy.entity.Person;
import org.apache.commons.compress.utils.Lists;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@SpringBootTest
class EsStudyApplicationTests {

    @Autowired
    private RestHighLevelClient restHighLevelClient;


    /**
     * 创建索引
     */
    @Test
    public void createIndex01() {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("cgg-index02");

        CreateIndexResponse createIndexResponse = null;

        try {
            createIndexResponse = restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println(createIndexResponse);
    }

    /**
     * 查询索引
     */
    @Test
    public void queryIndex01() {
        GetIndexRequest getIndexRequest = new GetIndexRequest("cgg-index01");

        boolean exist = false;
        try {
            //获取索引是否存在
            exist = restHighLevelClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(exist);
    }


    /**
     * 删除索引
     */
    @Test
    public void deleteIndex01() {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("cgg-index01");
        try {
            AcknowledgedResponse acknowledgedResponse = restHighLevelClient.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
            System.out.println(acknowledgedResponse.isAcknowledged());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 创建文档
     */
    @Test
    public void createDocument01() {

        Person person = new Person();
        person.setAge(26);
        person.setBirthDay(new Date());
        person.setId(1L);
        person.setName("love basketball haha");
        IndexRequest indexRequest = new IndexRequest("cgg-index01");
        indexRequest.id("00013");
        indexRequest.type("_doc");
        indexRequest.timeout(TimeValue.timeValueSeconds(1));
        indexRequest.source(JSON.toJSONString(person), XContentType.JSON);
        IndexResponse indexResponse = null;
        try {
            indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(indexResponse.status());
    }

    /**
     * 获取文档是否存在
     */
    @Test
    public void existDocument01() {
        GetRequest getRequest = new GetRequest("cgg-index01", "_doc", "00001");
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");
        boolean exist = false;
        try {
            exist = restHighLevelClient.exists(getRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(exist);

    }

    /**
     * 获取文档信息
     */
    @Test
    public void queryDocument01() {
        GetRequest getRequest = new GetRequest("cgg-index01");
        getRequest.id("00001");
        try {
            GetResponse getResponse = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
            System.out.println(getResponse.getSource());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 更新文档信息
     */
    @Test
    public void updateDocument01() {
        UpdateRequest updateRequest = new UpdateRequest("cgg-index01", "_doc", "00001");
        updateRequest.timeout("2s");
        // elasticsearch7之前的版本，springboot版本为2.X，因为elasticsearch7只能有一个类型，
        // 所以springboot中关于设置类型的都弃用了，
        // 默认类型使用_doc，而elasticsearch7之前的版本类型不能使用带下划线
//        updateRequest.type("_doc");
        Person person = new Person();
        person.setAge(26);
        person.setBirthDay(new Date());
        person.setId(1L);
        person.setName("cgg02-update202102191701");
        updateRequest.doc(JSON.toJSONString(person), XContentType.JSON);
        try {
            UpdateResponse updateResponse = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
            System.out.println(updateResponse.status());
            System.out.println(updateResponse.getGetResult());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 删除文档信息
     */
    @Test
    public void deleteDocument01() {
        DeleteRequest deleteRequest = new DeleteRequest("cgg-index02");
        deleteRequest.id("00002");
        DeleteResponse deleteResponse = null;
        try {
            deleteResponse = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
            System.out.println(deleteResponse.status());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 批量添加文档
     */
    @Test
    public void batchAddDocument01() {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("2s");
        List<Person> list = autoGenerationData(10);
        AtomicInteger atomicInteger = new AtomicInteger(1);
        for (Person person : list) {
            bulkRequest.add(new IndexRequest("cgg-index01").type("_doc").id(String.valueOf(atomicInteger.getAndIncrement()))
                    .source(JSON.toJSONString(person), XContentType.JSON));
        }
        try {
            BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            System.out.println(bulkResponse.hasFailures());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 多条件查询文档
     */
    @Test
    public void queryDocument02() {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //匹配所有,QueryBuilders有很多查询方法
        //QueryBuilders.matchAllQuery();
        //精确查询
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "love");
        TermQueryBuilder termQueryBuilder1 = QueryBuilders.termQuery("name", "basketball");
        boolQueryBuilder.must(termQueryBuilder);
        boolQueryBuilder.must(termQueryBuilder1);
        searchSourceBuilder.query(boolQueryBuilder);
        searchSourceBuilder.timeout(TimeValue.timeValueSeconds(1L));
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            System.out.println("hits:------------>");
            SearchHit[] hits = searchResponse.getHits().getHits();
            for (SearchHit hit : hits) {
                System.out.println("maps------>" + hit.getSourceAsMap());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


    }


    private List<Person> autoGenerationData(int total) {
        List<Person> list = Lists.newArrayList();
        for (int i = 0; i < total; i++) {
            Person person = new Person();
            person.setName("cgg0" + i);
            person.setId(new Date().getTime());
            person.setBirthDay(new Date());
            person.setAge(i * 2 + 10);
            list.add(person);
        }
        return list;
    }


}
