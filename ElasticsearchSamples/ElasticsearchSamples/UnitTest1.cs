using System;
using System.Collections.Generic;
using System.Linq;
using Elasticsearch.Net.ConnectionPool;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Nest;

namespace ElasticsearchSamples
{
    [TestClass]
    public class CreatingConnectionSampleTest
    {
        private ElasticClient myindexClient;

        [TestMethod]
        public void TestMethod1()
        {
            this.myindexClient = this.ConnectToSingleNode();

            var searchedTextItems = this.QueryByTerm();

            searchedTextItems = this.QueryByKeyword();

            searchedTextItems = this.QueryByMatch();

            searchedTextItems = this.QueryByMatchPhrase();

            searchedTextItems = this.QueryByMultipleConditions();

            searchedTextItems = this.QueryWithSorting();
        }

        private ElasticClient ConnectToSingleNode()
        {
            // 產生單一伺服器的連線設定，指定伺服器群及預設的 index 名稱。
            var connectionSetting = new ConnectionSettings(new Uri(@"http://elasticsearch01:9200"), "myindex0");

            // 建立 Elasticsearch Client，請重覆使用。
            return new ElasticClient(connectionSetting);
        }

        private ElasticClient ConnectToConnectionPool()
        {
            // 建立伺服器群
            var elasticNodes = new SniffingConnectionPool(new Uri[] { new Uri(@"http://elasticsearch01:9200"), new Uri(@"http://elasticsearch02:9200"), new Uri(@"http://elasticsearch03:9200") });

            // 產生連線設定，指定伺服器群及預設的 index 名稱。
            var connectionSetting = new ConnectionSettings(elasticNodes, "myindex0");

            // 建立 Elasticsearch Client，請重覆使用。
            return new ElasticClient(connectionSetting);
        }

        private IEnumerable<TextItem> QueryByTerm()
        {
            // 搜尋條件為 Id 這個欄位的值等於 2d50d70d-03f9-4769-8592-20de15bc6d0a
            return
                this.myindexClient.Search<TextItem>(s => s
                    .From(0)    // 從第 0 筆
                    .Size(10)   // 抓 10 筆
                    .Query(q =>
                        q.Term(p => p.Id, "2d50d70d-03f9-4769-8592-20de15bc6d0a")))
                .Hits
                .Select(h => h.Source);
        }

        private IEnumerable<TextItem> QueryByKeyword()
        {
            // 搜尋條件為 Content 這個欄位的值包含 "馬英九"
            return
                this.myindexClient.Search<TextItem>(s => s
                    .From(0)    // 從第 0 筆
                    .Size(10)   // 抓 10 筆
                    .Query(q =>
                        q.QueryString(qs => qs.OnFields(p => p.Content).Query("\"馬英九\""))))
                .Hits
                .Select(h => h.Source);
        }

        private IEnumerable<TextItem> QueryByMatch()
        {
            // 搜尋條件為 Content 這個欄位的值包含 "馬" "英" "九"
            //return
            //    this.myindexClient.Search<TextItem>(s => s
            //        .From(0)    // 從第 0 筆
            //        .Size(10)   // 抓 10 筆
            //        .Query(q =>
            //            q.QueryString(qs => qs.OnFields(p => p.Content).Query("馬英九"))))
            //    .Hits
            //    .Select(h => h.Source);

            // 上述條件相當於 Content 這個欄位的值 Match 馬英九
            return
                this.myindexClient.Search<TextItem>(s => s
                    .From(0)    // 從第 0 筆
                    .Size(10)   // 抓 10 筆
                    .Query(q =>
                        q.Match(qs => qs.OnField(p => p.Content).Query("馬英九"))))
                .Hits
                .Select(h => h.Source);
        }

        private IEnumerable<TextItem> QueryByMatchPhrase()
        {
            // 搜尋條件為 Content 這個欄位的值包含 "馬英九"
            //return
            //    this.myindexClient.Search<TextItem>(s => s
            //        .From(0)    // 從第 0 筆
            //        .Size(10)   // 抓 10 筆
            //        .Query(q =>
            //            q.QueryString(qs => qs.OnFields(p => p.Content).Query("\"馬英九\""))))
            //    .Hits
            //    .Select(h => h.Source);

            // 上述條件相當於 Content 這個欄位的值 MatchPhrase 馬英九
            return
                this.myindexClient.Search<TextItem>(s => s
                    .From(0)    // 從第 0 筆
                    .Size(10)   // 抓 10 筆
                    .Query(q =>
                        q.MatchPhrase(qs => qs.OnField(p => p.Content).Query("馬英九"))))
                .Hits
                .Select(h => h.Source);
        }

        private IEnumerable<TextItem> QueryByMultipleConditions()
        {
            // 搜尋條件為 Content 這個欄位的值包含 "馬英九" 而且也包含 "吳敦義"
            return
                this.myindexClient.Search<TextItem>(s => s
                    .From(0)    // 從第 0 筆
                    .Size(10)   // 抓 10 筆
                    .Query(q =>
                        q.QueryString(qs => qs.OnFields(p => p.Content).Query("\"馬英九\""))
                        && q.QueryString(qs => qs.OnFields(p => p.Content).Query("\"吳敦義\""))))
                .Hits
                .Select(h => h.Source);
        }

        private IEnumerable<TextItem> QueryWithSorting()
        {
            // 搜尋條件為 Content 這個欄位的值包含 "馬英九"，並且依 CreateTime 降冪排序搜尋結果。
            return
                this.myindexClient.Search<TextItem>(s => s
                    .From(0)    // 從第 0 筆
                    .Size(10)   // 抓 10 筆
                    .Query(q =>
                        q.QueryString(qs => qs.OnFields(p => p.Content).Query("\"馬英九\"")))
                    .SortDescending(p => p.CreatedTime))
                .Hits
                .Select(h => h.Source);
        }
    }
}
