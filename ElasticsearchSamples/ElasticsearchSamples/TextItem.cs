using System;

namespace ElasticsearchSamples
{
    internal class TextItem
    {
        public string AuthorId { get; internal set; }

        public string AuthorName { get; internal set; }

        public string Content { get; internal set; }

        public DateTime CreatedTime { get; internal set; }

        public Guid Id { get; internal set; }

        public DateTime ModifiedTime { get; internal set; }

        public string Summary { get; internal set; }
    }
}