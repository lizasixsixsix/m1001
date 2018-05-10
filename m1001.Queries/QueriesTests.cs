using System;
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
        private static IConfiguration Configuration { get; set; }

        private IMongoQueryable<Book> queryable;

        private IMongoCollection<Book> collection;

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

            collection = database.GetCollection<Book>(collName);

            queryable = collection.AsQueryable();
        }

        [TestMethod]
        public void _01_BooksAdded()
        {
            var books = collection.Find(
                BsonSerializer.Deserialize<BsonDocument>("{}")).ToList();

            Assert.IsTrue(books.Count == 5);

            Console.WriteLine(queryable.ToList()
                .Select(bk => bk.name + "\t" + bk.author + "\t" + bk.count
                              + "\t" + bk.genre.Aggregate((a, b) => a + ", " + b)
                              + "\t" + bk.year)
                .Aggregate((a, b) => a + "\n\n" + b));
        }

        private class NameOnly
        {
            public string name { get; set; }
        }

        [TestMethod]
        public void _02_BooksCountMoreThanOne()
        {
            var books = collection.Find(
                    BsonSerializer.Deserialize<BsonDocument>("{count: {$gt: 1}}"))
                .Project(
                    BsonSerializer.Deserialize<BsonDocument>("{_id: 0, name: 1}"))
                .Sort("{name: 1}")
                .ToList();

            Assert.IsTrue(books.Count == 4);

            var bookss = collection.Find(
                    BsonSerializer.Deserialize<BsonDocument>("{count: {$gt: 1}}"))
                .Limit(3)
                .ToList();

            Assert.IsTrue(bookss.Count == 3);

            Console.WriteLine(books
                .Select(bk => BsonSerializer.Deserialize<NameOnly>(bk).name)
                .Aggregate((a, b) => a + "\n\n" + b));

            Console.WriteLine(bookss.ToList()
                .Select(bk => bk.name + "\t" + bk.count)
                .Aggregate((a, b) => a + "\n\n" + b));
        }

        [TestMethod]
        public void _03_BooksWithMaxMinCount()
        {
            var bookMax = collection.Find(
                BsonSerializer.Deserialize<BsonDocument>("{}"))
                .Sort("{count: -1}").Limit(1).Single();

            Assert.IsTrue(bookMax.count == 11);

            var bookMin = collection.Find(
                BsonSerializer.Deserialize<BsonDocument>("{}"))
                .Sort("{count: 1}").Limit(1).Single();

            Assert.IsTrue(bookMin.count == 1);

            Console.WriteLine(bookMax.name + "\t" + bookMax.count
                              + "\n\n"
                              + bookMin.name + "\t" + bookMin.count);
        }

        [TestMethod]
        public void _04_DistinctAuthors()
        {
            var authors = collection.Distinct(
                new StringFieldDefinition<Book, string>("author"),
                BsonSerializer.Deserialize<BsonDocument>("{}"))
                .ToList();

            Assert.IsTrue(authors.Count == 2);

            Console.WriteLine(authors
                .Aggregate((a, b) => a + "\t" + b));
        }

        [TestMethod]
        public void _05_BooksWithoutAuthor()
        {
            var books = collection.Find(
                BsonSerializer.Deserialize<BsonDocument>("{author: {$exists: false}}"))
                .ToList();

            Assert.IsTrue(books.Count == 2);

            Console.WriteLine(books
                .Select(bk => bk.name)
                .Aggregate((a, b) => a + "\t" + b));
        }

        private class ReduceResult<T>
        {
            public string _id { get; set; }

            public T value { get; set; }
        }

        [TestMethod]
        public void _06_IncrementBooksCount()
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
                .MapReduce<ReduceResult<int>>(map, reduce)
                .Single().value;

            collection.UpdateMany(
                    BsonSerializer.Deserialize<BsonDocument>("{}"),
                    BsonSerializer.Deserialize<BsonDocument>("{$inc: {count: 1}}"));

            var newOverallCount = collection
                .MapReduce<ReduceResult<int>>(map, reduce)
                .Single().value;

            Assert.IsTrue(newOverallCount - oldOverallCount == queryable.Count());

            Console.WriteLine(queryable.ToList()
                .Select(bk => bk.name + "\t" + bk.count)
                .Aggregate((a, b) => a + "\n\n" + b));
        }

        [TestMethod]
        public void _07_AddNewGenre()
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

            Console.WriteLine(queryable.ToList()
                .Select(bk => bk.name + "\t" + bk.genre.Aggregate((a, b) => a + ", " + b))
                .Aggregate((a, b) => a + "\n\n" + b));
        }

        [TestMethod]
        public void _08_DeleteBooksWithCountLessThanThree()
        {
            var oldBooksCount = queryable.Count();

            collection.DeleteMany(
                BsonSerializer.Deserialize<BsonDocument>("{count: {$gt: 3}}"));

            var newBooksCount = queryable.Count();

            Assert.IsTrue(newBooksCount < oldBooksCount);

            Console.WriteLine(queryable.ToList()
                .Select(bk => bk.name + "\t" + bk.count)
                .Aggregate((a, b) => a + "\n\n" + b));
        }

        [TestMethod]
        public void _09_DeleteAllBooks()
        {
            collection.DeleteMany(
                BsonSerializer.Deserialize<BsonDocument>("{}"));

            Assert.IsFalse(queryable.Any());
        }
    }
}
