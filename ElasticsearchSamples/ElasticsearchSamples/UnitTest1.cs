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

            var searchedTextItems = this.QueryByKeyword();

            searchedTextItems = this.QueryByKeywordUseFields();

            this.DeleteDocuments();
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

        private IEnumerable<TextItemViewModel> QueryByKeyword()
        {
            // 搜尋條件為 Content 這個欄位的值包含 "馬英九"
            return
                this.myindexClient.Search<TextItemViewModel>(s => s
                    .From(0)    // 從第 0 筆
                    .Size(10)   // 抓 10 筆
                    .Type("textitem")
                    .Query(q =>
                        q.QueryString(qs => qs.OnFields(new string[] { "content" }).Query("\"馬英九\""))))
                .Hits
                .Select(h => h.Source);
        }

        private IEnumerable<TextItemViewModel> QueryByKeywordUseFields()
        {
            // 搜尋條件為 Content 這個欄位的值包含 "馬英九"
            return
                this.myindexClient.Search<TextItemViewModel>(s => s
                    .From(0)    // 從第 0 筆
                    .Size(10)   // 抓 10 筆
                    .Type("textitem")
                    .Query(q =>
                        q.QueryString(qs => qs.OnFields(new string[] { "content" }).Query("\"馬英九\"")))
                    .Fields(p => p.Id, p => p.AuthorId, p => p.AuthorName, p => p.Summary))
                .Hits
                .Where(h => h.Fields.FieldValuesDictionary != null && h.Fields.FieldValuesDictionary.Count > 0)
                .Select(h => ReflectTo<TextItemViewModel>(h.Fields.FieldValuesDictionary));
        }

        private void DeleteDocuments()
        {
            var deletedResult =
                this.myindexClient.DeleteByQuery<TextItem>(d =>
                    d.Query(q =>
                        q.Term(p => p.AuthorId, "123456")
                ));
        }

        private static T ReflectTo<T>(IDictionary<string, object> dict)
        {
            Type type = typeof(T);
            var obj = Activator.CreateInstance(type);

            foreach (var fv in dict)
            {
                var property = type.GetProperties().Where(p => p.Name.ToLower() == fv.Key.ToLower()).FirstOrDefault();

                if (property != null && fv.Value != null)
                {
                    property.SetValue(obj, ((Newtonsoft.Json.Linq.JArray)fv.Value).First.ToObject<object>());
                }
            }

            return (T)obj;
        }
    }
}
