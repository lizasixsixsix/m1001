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

namespace m1001.Linq
{
    [TestClass]
    public class LinqTests
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
            Assert.IsTrue(outCollection.Any());

            Assert.IsTrue(outCollection.Count() == 5);
        }

        [TestMethod]
        public void BooksCountMoreThanOne()
        {
            var books = outCollection.Where(b => b.count > 1);

            Assert.IsTrue(books.Count() == 4);
        }

        [TestMethod]
        public void BooksWithMaxMinCount()
        {
            var bookMax = outCollection.OrderByDescending(b => b.count).First();

            Assert.IsTrue(bookMax.count == 11);

            var bookMin = outCollection.OrderBy(b => b.count).First();

            Assert.IsTrue(bookMin.count == 1);
        }

        [TestMethod]
        public void DistinctAuthors()
        {
            var authors = outCollection.Where(b => b.author != null).Select(b => b.author).Distinct();

            Assert.IsTrue(authors.Count() == 2);
        }

        [TestMethod]
        public void BooksWithoutAuthor()
        {
            var books = outCollection.Where(b => b.author == null);

            Assert.IsTrue(books.Count() == 2);
        }

        [TestMethod]
        public void IncrementBooksCount()
        {
            var oldOverallCount = outCollection.Sum(b => b.count);

            outCollectionn.UpdateMany(Builders<Book>.Filter.Exists(b => b.count),
                Builders<Book>.Update.Inc(b => b.count, 1));

            var newOverallCount = outCollection.Sum(b => b.count);

            Assert.IsTrue(newOverallCount - oldOverallCount == outCollection.Count());
        }

        [TestMethod]
        public void AddNewGenre()
        {
            var oldGenreCount = outCollection.Select(b => b.genre.Length).Max();

            outCollectionn.UpdateMany(Builders<Book>.Filter.Where(b => b.genre.Contains("fantasy")),
                Builders<Book>.Update.AddToSet("genre", "favority"));

            var newGenreCount = outCollection.Select(b => b.genre.Length).Max();

            outCollectionn.UpdateMany(Builders<Book>.Filter.Where(b => b.genre.Contains("fantasy")),
                Builders<Book>.Update.AddToSet("genre", "favority"));

            var newerGenreCount = outCollection.Select(b => b.genre.Length).Max();

            Assert.IsTrue(newGenreCount > oldGenreCount);

            Assert.IsTrue(newerGenreCount == newGenreCount);
        }

        [TestMethod]
        public void DeleteBooksWithCountLessThanThree()
        {
            var oldBooksCount = outCollection.Count();

            outCollectionn.DeleteMany(Builders<Book>.Filter.Where(b => b.count < 3));

            var newBooksCount = outCollection.Count();

            Assert.IsTrue(newBooksCount < oldBooksCount);
        }

        [TestMethod]
        public void DeleteAllBooks()
        {
            outCollectionn.DeleteMany(Builders<Book>.Filter.Empty);

            Assert.IsFalse(outCollection.Any());
        }
    }
}
