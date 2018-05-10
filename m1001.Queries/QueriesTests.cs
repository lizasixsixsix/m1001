using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Extensions.Configuration;

using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using MongoDB.Driver.Linq;
using System.IO;
using System;
using m1001.Common;
using System.Linq;
using MongoDB.Driver.Builders;

namespace m1001.Queries
{
    [TestClass]
    public class QueriesTests
    {
        public static IConfiguration Configuration { get; set; }

        public IMongoQueryable<Book> outCollection;

        public IMongoCollection<Book> outCollectionn;

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

            var collection = database.GetCollection<BsonDocument>(collName);

            var data = File.ReadAllText("books.json");

            var document = BsonSerializer.Deserialize<BsonDocument>(data);

            var array = document[0].AsBsonArray;

            foreach (var element in array)
            {
                collection.InsertOne(element.AsBsonDocument);
            }

            outCollection = database.GetCollection<Book>(collName).AsQueryable();

            outCollectionn = database.GetCollection<Book>(collName);
        }

        [TestMethod]
        public void BooksAdded()
        {
            var books = outCollectionn.Find(BsonSerializer.Deserialize<BsonDocument>("{}")).ToList();

            Assert.IsTrue(books.Count == 5);
        }

        [TestMethod]
        public void BooksCountMoreThanOne()
        {
            var books = outCollectionn.Find(
                BsonSerializer.Deserialize<BsonDocument>("{count: {$gt: 1}}")).ToList();

            Assert.IsTrue(books.Count == 4);
        }

        [TestMethod]
        public void BooksWithMaxMinCount()
        {
            var bookMax = outCollectionn.Find(BsonSerializer.Deserialize<BsonDocument>("{}"))
                .Sort("{count: -1}").Limit(1).Single();

            Assert.IsTrue(bookMax.count == 11);

            var bookMin = outCollectionn.Find(BsonSerializer.Deserialize<BsonDocument>("{}"))
                .Sort("{count: 1}").Limit(1).Single();

            Assert.IsTrue(bookMin.count == 1);
        }

        [TestMethod]
        public void DistinctAuthors()
        {
            var authors = outCollectionn.Distinct(
                new StringFieldDefinition<Book, string>("author"),
                BsonSerializer.Deserialize<BsonDocument>("{}"))
                .ToList();

            Assert.IsTrue(authors.Count == 2);
        }

        [TestMethod]
        public void BooksWithoutAuthor()
        {
            var books = outCollectionn.Find(
                BsonSerializer.Deserialize<BsonDocument>("{author: {$exists: false}}")).ToList();

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

            var oldOverallCount = outCollectionn.MapReduce<SimpleReduceResult<int>>(map, reduce)
                .Single().value;

            outCollectionn.UpdateMany(
                    BsonSerializer.Deserialize<BsonDocument>("{}"),
                    BsonSerializer.Deserialize<BsonDocument>("{$inc: {count: 1}}"));

            var newOverallCount = outCollectionn.MapReduce<SimpleReduceResult<int>>(map, reduce)
                .Single().value;

            Assert.IsTrue(newOverallCount - oldOverallCount == outCollection.Count());
        }

        [TestMethod]
        public void AddNewGenre()
        {
            var oldGenreCount = outCollection.Select(b => b.genre.Length).Max();

            outCollectionn.UpdateMany(BsonSerializer.Deserialize<BsonDocument>("{genre: 'fantasy'}"),
                BsonSerializer.Deserialize<BsonDocument>("{$addToSet:{genre: 'favority'}}"));

            var newGenreCount = outCollection.Select(b => b.genre.Length).Max();

            outCollectionn.UpdateMany(BsonSerializer.Deserialize<BsonDocument>("{genre: 'fantasy'}"),
                BsonSerializer.Deserialize<BsonDocument>(
                    "{$addToSet:{genre: 'favority'}}"));

            var newerGenreCount = outCollection.Select(b => b.genre.Length).Max();

            Assert.IsTrue(newGenreCount > oldGenreCount);

            Assert.IsTrue(newerGenreCount == newGenreCount);
        }

        [TestMethod]
        public void DeleteBooksWithCountLessThanThree()
        {
            var oldBooksCount = outCollection.Count();

            outCollectionn.DeleteMany(BsonSerializer.Deserialize<BsonDocument>("{count: {$gt: 3}}"));

            var newBooksCount = outCollection.Count();

            Assert.IsTrue(newBooksCount < oldBooksCount);
        }

        [TestMethod]
        public void DeleteAllBooks()
        {
            outCollectionn.DeleteMany(BsonSerializer.Deserialize<BsonDocument>("{}"));

            Assert.IsFalse(outCollection.Any());
        }
    }
}
