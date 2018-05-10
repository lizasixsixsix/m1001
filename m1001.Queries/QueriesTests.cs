using System.IO;
using System.Linq;

using Microsoft.Extensions.Configuration;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using MongoDB.Driver.Linq;

using m1001.Common;

namespace m1001.Queries
{
    [TestClass]
    public class QueriesTests
    {
        public static IConfiguration Configuration { get; set; }

        public IMongoQueryable<Book> queryable;

        public IMongoCollection<Book> collection;

        [TestInitialize]
        public void Initialize()
        {
            var builder = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.real.json");

            Configuration = builder.Build();

            MongoUrl url = new MongoUrlBuilder
            {
                Server = new MongoServerAddress(Configuration["db:server"]),
                DatabaseName = Configuration["db:database"],
                Username = Configuration["db:username"],
                Password = Configuration["db:password"]
            }
            .ToMongoUrl();

            MongoClient client = new MongoClient(url);

            IMongoDatabase database = client.GetDatabase("mentoring");

            var collName = "Books";

            database.DropCollection(collName);

            if (database.GetCollection<BsonDocument>(collName) == null)
            {

                database.CreateCollection(collName);
            }

            var coll = database.GetCollection<BsonDocument>(collName);

            var data = File.ReadAllText("books.json");

            var document = BsonSerializer.Deserialize<BsonDocument>(data);

            var array = document[0].AsBsonArray;

            foreach (var element in array)
            {
                coll.InsertOne(element.AsBsonDocument);
            }

            queryable = database.GetCollection<Book>(collName).AsQueryable();

            collection = database.GetCollection<Book>(collName);
        }

        [TestMethod]
        public void BooksAdded()
        {
            var books = collection.Find(
                BsonSerializer.Deserialize<BsonDocument>("{}")).ToList();

            Assert.IsTrue(books.Count == 5);
        }

        [TestMethod]
        public void BooksCountMoreThanOne()
        {
            var books = collection.Find(
                BsonSerializer.Deserialize<BsonDocument>("{count: {$gt: 1}}"))
                .ToList();

            Assert.IsTrue(books.Count == 4);
        }

        [TestMethod]
        public void BooksWithMaxMinCount()
        {
            var bookMax = collection.Find(
                BsonSerializer.Deserialize<BsonDocument>("{}"))
                .Sort("{count: -1}").Limit(1).Single();

            Assert.IsTrue(bookMax.count == 11);

            var bookMin = collection.Find(
                BsonSerializer.Deserialize<BsonDocument>("{}"))
                .Sort("{count: 1}").Limit(1).Single();

            Assert.IsTrue(bookMin.count == 1);
        }

        [TestMethod]
        public void DistinctAuthors()
        {
            var authors = collection.Distinct(
                new StringFieldDefinition<Book, string>("author"),
                BsonSerializer.Deserialize<BsonDocument>("{}"))
                .ToList();

            Assert.IsTrue(authors.Count == 2);
        }

        [TestMethod]
        public void BooksWithoutAuthor()
        {
            var books = collection.Find(
                BsonSerializer.Deserialize<BsonDocument>("{author: {$exists: false}}"))
                .ToList();

            Assert.IsTrue(books.Count == 2);
        }

        public class SimpleReduceResult<T>
        {
            public string Id { get; set; }

            public T value { get; set; }
        }

        [TestMethod]
        public void IncrementBooksCount()
        {
            var map = new BsonJavaScript(
                @"
                function()
                {
                    emit('a', this.count)
                }");

            var reduce = new BsonJavaScript(
                @"
                function(key, values)
                {
                    return Array.sum(values)
                }");

            var oldOverallCount = collection
                .MapReduce<SimpleReduceResult<int>>(map, reduce)
                .Single().value;

            collection.UpdateMany(
                    BsonSerializer.Deserialize<BsonDocument>("{}"),
                    BsonSerializer.Deserialize<BsonDocument>("{$inc: {count: 1}}"));

            var newOverallCount = collection
                .MapReduce<SimpleReduceResult<int>>(map, reduce)
                .Single().value;

            Assert.IsTrue(newOverallCount - oldOverallCount == queryable.Count());
        }

        [TestMethod]
        public void AddNewGenre()
        {
            var oldGenreCount = queryable.Select(b => b.genre.Length).Max();

            collection.UpdateMany(
                BsonSerializer.Deserialize<BsonDocument>("{genre: 'fantasy'}"),
                BsonSerializer.Deserialize<BsonDocument>("{$addToSet: {genre: 'favority'}}"));

            var newGenreCount = queryable.Select(b => b.genre.Length).Max();

            collection.UpdateMany(
                BsonSerializer.Deserialize<BsonDocument>("{genre: 'fantasy'}"),
                BsonSerializer.Deserialize<BsonDocument>("{$addToSet: {genre: 'favority'}}"));

            var newerGenreCount = queryable.Select(b => b.genre.Length).Max();

            Assert.IsTrue(newGenreCount > oldGenreCount);

            Assert.IsTrue(newerGenreCount == newGenreCount);
        }

        [TestMethod]
        public void DeleteBooksWithCountLessThanThree()
        {
            var oldBooksCount = queryable.Count();

            collection.DeleteMany(
                BsonSerializer.Deserialize<BsonDocument>("{count: {$gt: 3}}"));

            var newBooksCount = queryable.Count();

            Assert.IsTrue(newBooksCount < oldBooksCount);
        }

        [TestMethod]
        public void DeleteAllBooks()
        {
            collection.DeleteMany(
                BsonSerializer.Deserialize<BsonDocument>("{}"));

            Assert.IsFalse(queryable.Any());
        }
    }
}
