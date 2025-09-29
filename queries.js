const { MongoClient } = require("mongodb");
const uri = "mongodb://localhost:27017";
const dbName = "plp_bookstore";

async function runQueries() {
  const client = new MongoClient(uri);
  try {
    await client.connect();
    const db = client.db(dbName);
    const books = db.collection("books");

    console.log("Connected to MongoDB\n");

    // ----------------------
    // TASK 2 – Basic CRUD
    // ----------------------

    // 1. Find all books in a specific genre
    const fiction = await books.find({ genre: "Fiction" }).toArray();
    console.log("Task 2.1 - Fiction Books:", fiction);

    // 2. Find books published after a certain year
    const after1950 = await books.find({ published_year: { $gt: 1950 } }).toArray();
    console.log("Task 2.2 - Books after 1950:", after1950);

    // 3. Find books by a specific author
    const orwell = await books.find({ author: "George Orwell" }).toArray();
    console.log("Task 2.3 - Books by George Orwell:", orwell);

    // 4. Update the price of a specific book
    await books.updateOne({ title: "1984" }, { $set: { price: 15.99 } });
    console.log("Task 2.4 - Updated price for 1984");

    // 5. Delete a book by its title
    await books.deleteOne({ title: "Moby Dick" });
    console.log("Task 2.5 - Deleted Moby Dick");

    // ----------------------
    // TASK 3 – Advanced Queries
    // ----------------------

    // 1. Find books in stock and published after 2010
    const recentStock = await books.find({ in_stock: true, published_year: { $gt: 2010 } }).toArray();
    console.log("Task 3.1 - In-stock books after 2010:", recentStock);

    // 2. Projection: return only title, author, and price
    const projection = await books.find({}, { projection: { title: 1, author: 1, price: 1, _id: 0 } }).toArray();
    console.log("Task 3.2 - Projection (title, author, price):", projection);

    // 3. Sorting by price
    const ascending = await books.find().sort({ price: 1 }).toArray();
    console.log("Task 3.3 - Books sorted by price ascending:", ascending);

    const descending = await books.find().sort({ price: -1 }).toArray();
    console.log("Task 3.3 - Books sorted by price descending:", descending);

    // 4. Pagination (5 books per page)
    const page1 = await books.find().skip(0).limit(5).toArray();
    console.log("Task 3.4 - Page 1 (first 5 books):", page1);

    const page2 = await books.find().skip(5).limit(5).toArray();
    console.log("Task 3.4 - Page 2 (next 5 books):", page2);

    // ----------------------
    // TASK 4 – Aggregation Pipelines
    // ----------------------

    // 1. Average price of books by genre
    const avgByGenre = await books.aggregate([
      { $group: { _id: "$genre", avgPrice: { $avg: "$price" } } }
    ]).toArray();
    console.log("Task 4.1 - Average price by genre:", avgByGenre);

    // 2. Author with the most books
    const topAuthor = await books.aggregate([
      { $group: { _id: "$author", bookCount: { $sum: 1 } } },
      { $sort: { bookCount: -1 } },
      { $limit: 1 }
    ]).toArray();
    console.log("Task 4.2 - Author with most books:", topAuthor);

    // 3. Group books by publication decade
    const byDecade = await books.aggregate([
      {
        $group: {
          _id: { $floor: { $divide: ["$published_year", 10] } },
          count: { $sum: 1 }
        }
      },
      {
        $project: {
          decade: { $multiply: ["$_id", 10] },
          count: 1,
          _id: 0
        }
      },
      { $sort: { decade: 1 } }
    ]).toArray();
    console.log("Task 4.3 - Books grouped by decade:", byDecade);

    // ----------------------
    // TASK 5 – Indexing
    // ----------------------

    // 1. Create an index on title
    await books.createIndex({ title: 1 });
    console.log("Task 5.1 - Index created on title");

    // 2. Create a compound index on author and published_year
    await books.createIndex({ author: 1, published_year: -1 });
    console.log("Task 5.2 - Compound index created on author + published_year");

    // 3. Use explain() to demonstrate performance
    const explainResult = await books.find({ title: "1984" }).explain("executionStats");
    console.log("Task 5.3 - Explain output for title search:", explainResult);

  } finally {
    await client.close();
    console.log("\nConnection closed");
  }
}

runQueries().catch(console.error);
