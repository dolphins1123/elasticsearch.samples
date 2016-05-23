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
        public void Test_CreateSingleNodeConnection()
        {
            this.myindexClient = this.ConnectToSingleNode();

            this.IndexDocument();

            this.IndexMultipleDocuments();
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

        private void IndexDocument()
        {
            // 假設有一個 TextItem 的 object
            TextItem textItem = new TextItem()
            {
                Id = Guid.NewGuid(),
                Summary = "I'm summary.",
                Content = "I'm content.",
                AuthorId = "99999",
                AuthorName = "Dotblogs",
                CreatedTime = DateTime.Now,
                ModifiedTime = DateTime.Now
            };

            // 直接呼叫 Index 將 object 給入即可。
            // 官網建議自行指定 document Id，不指定的話預設 Elasticsearch 會自己給。
            // type 如果不指定，預設就是類別名稱。
            this.myindexClient.Index(textItem, indexDescriptor => indexDescriptor.Id(textItem.Id.ToString()));
        }

        private void IndexMultipleDocuments()
        {
            // 假設我產生了多個 TextItem
            List<TextItem> textItems = CreateTextItems();

            // 呼叫 Bulk 方法，給入一個 BulkRequest object，指定 BulkRequest.Operations 為 List<IBulkOperation<TextItem>>。
            this.myindexClient.Bulk(new BulkRequest()
            {
                Operations = textItems.Select(t => new BulkIndexOperation<TextItem>(t) { Id = t.Id.ToString() } as IBulkOperation).ToList()
            });
        }

        private static List<TextItem> CreateTextItems()
        {
            List<TextItem> textItems = new List<TextItem>();

            for (int index = 0; index < 10; index++)
            {
                textItems.Add(
                    new TextItem()
                    {
                        Id = Guid.NewGuid(),
                        Summary = "I'm summary.",
                        Content = "I'm content.",
                        AuthorId = "99999",
                        AuthorName = "Dotblogs",
                        CreatedTime = DateTime.Now,
                        ModifiedTime = DateTime.Now
                    });
            }

            return textItems;
        }
    }
}
