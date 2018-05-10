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
            Assert.IsTrue(queryable.Any());

            Assert.IsTrue(queryable.Count() == 5);
        }

        [TestMethod]
        public void BooksCountMoreThanOne()
        {
            var books = queryable.Where(b => b.count > 1);

            Assert.IsTrue(books.Count() == 4);
        }

        [TestMethod]
        public void BooksWithMaxMinCount()
        {
            var bookMax = queryable.OrderByDescending(b => b.count).First();

            Assert.IsTrue(bookMax.count == 11);

            var bookMin = queryable.OrderBy(b => b.count).First();

            Assert.IsTrue(bookMin.count == 1);
        }

        [TestMethod]
        public void DistinctAuthors()
        {
            var authors = queryable.Where(b => b.author != null)
                .Select(b => b.author).Distinct();

            Assert.IsTrue(authors.Count() == 2);
        }

        [TestMethod]
        public void BooksWithoutAuthor()
        {
            var books = queryable.Where(b => b.author == null);

            Assert.IsTrue(books.Count() == 2);
        }

        [TestMethod]
        public void IncrementBooksCount()
        {
            var oldOverallCount = queryable.Sum(b => b.count);

            collection.UpdateMany(
                Builders<Book>.Filter.Empty,
                Builders<Book>.Update.Inc(b => b.count, 1));

            var newOverallCount = queryable.Sum(b => b.count);

            Assert.IsTrue(newOverallCount - oldOverallCount == queryable.Count());
        }

        [TestMethod]
        public void AddNewGenre()
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
        }

        [TestMethod]
        public void DeleteBooksWithCountLessThanThree()
        {
            var oldBooksCount = queryable.Count();

            collection.DeleteMany(
                Builders<Book>.Filter.Where(b => b.count < 3));

            var newBooksCount = queryable.Count();

            Assert.IsTrue(newBooksCount < oldBooksCount);
        }

        [TestMethod]
        public void DeleteAllBooks()
        {
            collection.DeleteMany(
                Builders<Book>.Filter.Empty);

            Assert.IsFalse(queryable.Any());
        }
    }
}
