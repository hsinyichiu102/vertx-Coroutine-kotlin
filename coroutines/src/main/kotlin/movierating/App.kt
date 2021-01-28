package movierating

import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.MongoClient
import io.vertx.ext.web.Route
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.launch




/**
 * version: 2021-01-27
 * author: HsinYi
 * 1. a test to replace the JDBC to MongoDB by using Kotlin coroutine
 * 2. using the original enviorment from JDBC but swap to MongoDB
 * 3. adding two new API: a. router.put("/movie/:id")b. router.delete("/movie/:id")
 *
 * JDBC code from: https://github.com/vert-x3/vertx-examples/tree/4.x/kotlin-examples/coroutines
 */

class App : CoroutineVerticle() {

  private lateinit var client: MongoClient

  override suspend fun start() {
    /* create the connection to JDBC*/
    var config = Vertx.currentContext().config()
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


   /*create the collection and insert some original data inside*/
    var product= json {
      obj(
              "movie_id" to "indianajones",
              "title" to "Indiana Jones",
              "rating" to listOf(4, 6, 3, 9))}

    client.save("movie", product).await()

    var product2 = json{
      obj(
              "movie_id" to "starwars",
              "title" to "Star Wars",
              "rating" to listOf(1, 5, 9, 10))}
    client.insert("movie", product2).await()
    print("document saved")


    /* !problem: tried to put all the objects in one json as below,
    *           but only the last document("indianajones") can be stored in the collection,
    *           therefore the documents need to be stored and saved separately as above
    var product = json{
      obj(
              "movie_id" to "starwars",
              "title" to "Star Wars",
              "rating" to listOf(1,5,9,10))
     obj(
          "movie_id" to "indianajones",
          "title" to "Indiana Jones",
          "rating" to listOf(4,6,3,9))}
    client.insert("movie",product){id->print("Inserted id: ${id.result()}")}

    print("document saved")*/

      // Build Vert.x Web router
      val router = Router.router(vertx)
      router.get("/movie/:id").coroutineHandler { ctx -> getMovie(ctx) }
      router.post("/rateMovie/:id").coroutineHandler { ctx -> rateMovie(ctx) }// post: add a document
      router.get("/getRating/:id").coroutineHandler { ctx -> getRating(ctx) }// get: get a document or status
      router.put("/movie/:id").coroutineHandler { ctx -> putMovie(ctx) } // put: using UPDATE to add a new document at a selected position
      router.delete("/movie/:id").coroutineHandler { ctx -> deleteMovie(ctx) }// delete: delete a selected document

      // Start the server
      vertx.createHttpServer()
              .requestHandler(router)
              .listen(config.getInteger("http.port", 8080))
              .await()
    }

  /**
   * putMovie: to update a new movie into the movie collection
   */
  suspend fun putMovie(ctx: RoutingContext) {
      val movie_id = ctx.pathParam("movie_id") //
      val query = JsonObject().put("movie_id", movie_id)
      val update= json{
      obj(
              "movie_id" to movie_id,
              "title" to ctx.queryParam("title")[0],
              "rating" to ctx.queryParam("rating")[0]

      )}

      val result= client.findOneAndUpdate("movie", query, update).await()
      if(result.containsKey(movie_id)){
          ctx.response().setStatusCode(200).end()
      }
      else{
          ctx.response().setStatusCode(404).end()
      }

  }

  /**
   * deleteMovie: to delete from the collection
   */
  suspend fun deleteMovie(ctx: RoutingContext) {
      val movie_id = ctx.pathParam("movie_id")
      val query = JsonObject().put("movie_id", movie_id)

      client.removeDocument("movie", query).await()
      ctx.response().setStatusCode(200).end()
      }



  /**
   * getMovie: to find out a document from the collection
   */
  suspend fun getMovie(ctx: RoutingContext) {
      val movie_id = ctx.pathParam("movie_id")

      val query = JsonObject().put("movie_id", movie_id)
      val result = client.find("movie", query).await()
      if(result.contains(movie_id)){
          ctx.response().end(json {
              obj("movie_id" to movie_id,
                      "title" to result.get(0).getString("title")).encode()
          })
      }else{
          ctx.response().setStatusCode(404).end()
      }
    }

  /**
   * rateMovie: to add the rating score to the collection in rating key
   * 1. to find the movie and get the list of the rating number
   * 2. adding the new rating score from the front-end to the rating list
   * 3. using the helper "insertRating" to update the document
   */

  suspend fun rateMovie(ctx: RoutingContext) {
      val movie_id = ctx.pathParam("movie_id")
      val rating = Integer.parseInt(ctx.queryParam("rating")[0])
      val query = JsonObject().put("movie_id", movie_id)

      val find_result = client.find("movie", query).await()

      var rating_update:JsonArray

      if(find_result.contains(movie_id)){
          rating_update= find_result.get(0).getJsonArray("rating").add(rating)

          val result= client.findOneAndUpdate("movie", query, JsonObject().put("rating", rating_update)).await()
          if(result.containsKey(movie_id)){
              ctx.response().setStatusCode(200).end()
          }else{
              ctx.response().setStatusCode(404).end()
          }
      }else{
          ctx.response().setStatusCode(500).end("cannot find the movie")
      }


  }

  /**
   * getRating: getting the average score of the movie_id
   * 1. using asIterable() to iterate the element in the JsonArray
   * 2. sum all the score and caculate the average
   * 3. response only the average result
   */
  suspend fun getRating(ctx: RoutingContext) {
      val movie_id = ctx.pathParam("movie_id")
      val query = JsonObject().put("movie_id", movie_id)
      val rating_arr=client.find("movie", query).await()
      val score = client.aggregate("rating_avg",rating_arr.get(0).getJsonArray("rating"))

      ctx.response().end(json{
          obj(
                  "movie_id" to movie_id,
                  "rating_avg" to score)
          .encode()
      })
    }

    /**
     * An extension method for simplifying coroutines usage with Vert.x Web routers
     */
    fun Route.coroutineHandler(fn: suspend (RoutingContext) -> Unit) {
      handler { ctx ->
        launch(ctx.vertx().dispatcher()) {
          try {
            fn(ctx)
          } catch (e: Exception) {
            ctx.fail(e)
          }
        }
      }
    }
  }










