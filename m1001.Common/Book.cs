using MongoDB.Bson;

namespace m1001.Common
{
    public class Book
    {
        public ObjectId _id { get; set; }

        public string name { get; set; }

        public string author { get; set; }

        public int count { get; set; }

        public string genre { get; set; }

        public int year { get; set; }
    }
}
