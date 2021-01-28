#Vertx Coroutine Kotlin


A test to swap the JDBC to MongoDB written by Kotlin.
All the enviornment were refered from https://github.com/vert-x3/vertx-examples/tree/4.x/kotlin-examples/coroutines

## establish the connection with MongoDB 

>     var config = Vertx.currentContext().config()
    var uri = config.getString("mongo_uri")
    if (uri == null) {
      uri = "mongodb://localhost:27017"
    }
    var db = config.getString("mongo_db")
    if (db == null) {
      db = "test"
    }
    val mongoconfig = JsonObject().put("connection_string", uri).put("db_name", db)

    client = MongoClient.createShared(vertx, mongoconfig)

## intial document in the MongoDB

> var product= json {
      obj(
          "movie_id" to "indianajones",
          "title" to "Indiana Jones",
          "rating" to listOf(4,6,3,9))}

    client.save("movie",product){id->print("Inserted id: ${id.result()}")}

    var product2 = json{
      obj(
              "movie_id" to "starwars",
              "title" to "Star Wars",
              "rating" to listOf(1,5,9,10))}
    client.insert("movie",product2){id->print("Inserted id: ${id.result()}")}
    print("document saved")

## Route API setting with 5 action

> val router = Router.router(vertx)
      router.get("/movie/:id").coroutineHandler { ctx -> getMovie(ctx) }
      router.post("/rateMovie/:id").coroutineHandler { ctx -> rateMovie(ctx) }// post: add a document
      router.get("/getRating/:id").coroutineHandler { ctx -> getRating(ctx) }// get: get a document or status
      router.put("/movie/:id").coroutineHandler { ctx -> putMovie(ctx) } // put: using UPDATE to add a new document at a selected position
      router.delete("/movie/:id").coroutineHandler { ctx -> deleteMovie(ctx) }// delete: delete a selected document

## explianation of 5 main methods

1. putMovie: to put a new movie document into the collection
2. deleteMovie: to delete a movie document from the collection
3. getMovie: to find the specific movie from the collection
4. rateMovie: to provide a score of a movie document
5. getRating: to get the average of the rating score
