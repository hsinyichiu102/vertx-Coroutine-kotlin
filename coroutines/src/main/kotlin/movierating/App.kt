package movierating

import io.vertx.core.Vertx
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
          "rating" to listOf(4,6,3,9))}

    client.save("movie",product){id->print("Inserted id: ${id.result()}")}

    var product2 = json{
      obj(
              "movie_id" to "starwars",
              "title" to "Star Wars",
              "rating" to listOf(1,5,9,10))}
    client.insert("movie",product2){id->print("Inserted id: ${id.result()}")}
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
   * putMovie: to put a new movie into the movie collection
   */
  suspend fun putMovie(ctx: RoutingContext) {
      val movie_id = ctx.request().params().get("movie_id") //
      val title = ctx.request().params().get("title")  // assume that title and id are received and it is totally new one in the collection
      val movie = JsonObject().put("title", title).put("movie_id", movie_id)
      val response = JsonObject()
      client.insert("movie", movie) { res ->
        if (res.succeeded()) {
          response.put("success", true)
                  .put("movie", res.result())

          ctx.response().setStatusCode(200)
                  .putHeader("Content-Type", "application/json")
                  .end(response.encode())
        } else {
          //ctx.response().end(res.cause().toString())
          response.put("success", false)
                  .put("error", res.cause().message)

          ctx.response().setStatusCode(400)
                  .putHeader("Content-Type", "application/json")
                  .end(response.encode())
        }
      }
    }

  /**
   * deleteMovie: to delete a document from the collection
   */
  suspend fun deleteMovie(ctx: RoutingContext) {
      val movie_id = ctx.request().params().get("movie_id")
      val title = ctx.request().params().get("title")
      val query = JsonObject().put("title", title).put("movie_id", movie_id)
      client.removeDocument("movie", query) { res ->
        if (res.succeeded()) {
          ctx.response().setStatusCode(200)
                  .putHeader("Content-Type", "application/json")
                  .end("Deleted successfully")
        } else {
          ctx.response().setStatusCode(500)
                  .end("Failed to delete the movie")
        }
      }
    }


  /**
   * getMovie: to find out a document from the collection
   */
  suspend fun getMovie(ctx: RoutingContext) {
      val movie_id = ctx.request().params().get("movie_id")
      val title = ctx.request().params().get("title")
      val query = JsonObject().put("title", title).put("movie_id", movie_id)
      val response = JsonObject()
      client.find("movie", query) { res ->
        if (res.succeeded()) {
          response.put("success", true)
                  .put("movie", res.result())

          ctx.response().setStatusCode(200)
                  .putHeader("Content-Type", "application/json")
                  .end(response.encode())

        } else {
          response.put("success", false)
                  .put("error", res.cause().message)

          ctx.response().setStatusCode(400)
                  .putHeader("Content-Type", "application/json")
                  .end(response.encode())
        }
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
      val query = JsonObject().put("movie_id", movie_id)
      var response = JsonObject()
      client.find("movie",query){res->
        if(res.succeeded()){
          val arr = res.result().get(0).getJsonArray("rating").toList()
          insertRating(query,arr,ctx)
        }
        else{
          response.put("success", false)
                  .put("error", res.cause().message)

          ctx.response().setStatusCode(400)
                  .putHeader("Content-Type", "application/json")
                  .end(response.encode())
        }
      }
    }

  /**
   * insertRating: a helper method to update the rating result to the document
   * and response the success result back
   * !bug here: always get trouble when using findOneAndUpdate
   */
  fun insertRating(query:JsonObject, arr:List<Any>,ctx: RoutingContext){
      var response = JsonObject()

      client.findOneAndUpdate("movie",query,JsonObject().put("rating", arr)){res->
        if(res.succeeded()){
          response.put("success",true).put("movie",res.result())

          ctx.response().setStatusCode(200)
                  .putHeader("Content-Type", "application/json")
                  .end(response.encode())
        }else{
        response.put("success", false)
                .put("error", res.cause().message)

        ctx.response().setStatusCode(400)
                .putHeader("Content-Type", "application/json")
                .end(response.encode())
        }
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
      var response = JsonObject()
      client.find("movie", query) { res ->
        if (res.succeeded()) {
          val arr = res.result().get(0).getJsonArray("rating").asIterable()
          var sum=0.0
          var length=0.0
          for(i in arr){
            length++
            sum+= i.toString().toInt()
          }
          val average= sum/length

          response.put("succee",true).put("movie",JsonObject().put("avg",average))

          ctx.response().setStatusCode(200)
                  .putHeader("Content-Type", "application/json")
                  .end(response.encode())

        }else{
          response.put("success", false)
                  .put("error", res.cause().message)

          ctx.response().setStatusCode(400)
                  .putHeader("Content-Type", "application/json")
                  .end(response.encode())
        }
      }
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





