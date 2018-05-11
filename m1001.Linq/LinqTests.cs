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

namespace m1001.Linq
{
    [TestClass]
    public class LinqTests
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

            IMongoDatabase database = client.GetDatabase(Configuration["db:database"]);

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
            Assert.IsTrue(queryable.Any());

            Assert.IsTrue(queryable.Count() == 5);

            Console.WriteLine(queryable.ToList()
                .Select(bk => bk.name + "\t" + bk.author + "\t" + bk.count
                              + "\t" + bk.genre.Aggregate((a, b) => a + ", " + b)
                              + "\t" + bk.year)
                .Aggregate((a, b) => a + "\n\n" + b));
        }

        [TestMethod]
        public void _02_BooksCountMoreThanOne()
        {
            var books = queryable.Where(b => b.count > 1)
                .Select(b => new { b.name })
                .OrderBy(bb => bb.name);

            Assert.IsTrue(books.Count() == 4);

            var bookss = queryable.Where(b => b.count > 1)
                .Take(3);

            Assert.IsTrue(bookss.Count() == 3);

            Console.WriteLine(books.ToList()
                .Select(bk => bk.name)
                .Aggregate((a, b) => a + "\n\n" + b));

            Console.WriteLine(bookss.ToList()
                .Select(bk => bk.name + "\t" + bk.count)
                .Aggregate((a, b) => a + "\n\n" + b));
        }

        [TestMethod]
        public void _03_BooksWithMaxMinCount()
        {
            var bookMax = queryable.OrderByDescending(b => b.count).First();

            Assert.IsTrue(bookMax.count == 11);

            var bookMin = queryable.OrderBy(b => b.count).First();

            Assert.IsTrue(bookMin.count == 1);

            Console.WriteLine(bookMax.name + "\t" + bookMax.count
                              + "\n\n"
                              + bookMin.name + "\t" + bookMin.count);
        }

        [TestMethod]
        public void _04_DistinctAuthors()
        {
            var authors = queryable.Where(b => b.author != null)
                .Select(b => b.author).Distinct();

            Assert.IsTrue(authors.Count() == 2);

            Console.WriteLine(authors.ToList()
                .Aggregate((a, b) => a + "\t" + b));
        }

        [TestMethod]
        public void _05_BooksWithoutAuthor()
        {
            var books = queryable.Where(b => b.author == null);

            Assert.IsTrue(books.Count() == 2);

            Console.WriteLine(books.ToList()
                .Select(bk => bk.name)
                .Aggregate((a, b) => a + "\t" + b));
        }

        [TestMethod]
        public void _06_IncrementBooksCount()
        {
            var oldOverallCount = queryable.Sum(b => b.count);

            collection.UpdateMany(
                Builders<Book>.Filter.Empty,
                Builders<Book>.Update.Inc(b => b.count, 1));

            var newOverallCount = queryable.Sum(b => b.count);

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
                Builders<Book>.Filter.Where(b => b.genre.Contains("fantasy")),
                Builders<Book>.Update.AddToSet("genre", "favority"));

            var newGenreCount = queryable.Select(b => b.genre.Length).Max();

            collection.UpdateMany(
                Builders<Book>.Filter.Where(b => b.genre.Contains("fantasy")),
                Builders<Book>.Update.AddToSet("genre", "favority"));

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
                Builders<Book>.Filter.Where(b => b.count < 3));

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
                Builders<Book>.Filter.Empty);

            Assert.IsFalse(queryable.Any());
        }
    }
}
