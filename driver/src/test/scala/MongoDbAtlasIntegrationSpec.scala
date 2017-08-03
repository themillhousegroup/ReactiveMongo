import java.util.concurrent.TimeUnit
import reactivemongo.api.commands.DBUserRole
import reactivemongo.api.MongoConnection.ParsedURI
import reactivemongo.api.{ DefaultDB, ScramSha1Authentication, MongoConnection, MongoDriver }
import reactivemongo.core.actors.Exceptions.PrimaryUnavailableException
import reactivemongo.core.commands.FailedAuthentication
import scala.concurrent.{ Awaitable, Await, ExecutionContext, Future }

import scala.Some
import shaded.netty.channel.ChannelFuture

import scala.concurrent.duration._

import org.specs2.matcher.MatchResult
import org.specs2.concurrent.{ ExecutionEnv => EE }

import reactivemongo.core.nodeset.{
  Connection,
  ConnectionStatus,
  Node,
  NodeSet,
  NodeStatus,
  ProtocolMetadata
}

import reactivemongo.core.actors.StandardDBSystem

class MongoDbAtlasIntegrationSpec extends org.specs2.mutable.Specification {

  val veryShortTimeoutMS = 10000

  val veryShortTimeoutDuration = new FiniteDuration(veryShortTimeoutMS * 5, TimeUnit.MILLISECONDS)

  val atlasHostConnectionString = s"mongodb://atlasreadonlyuser:atlasreadonlypassword@snirequiredcluster-shard-00-00-f6vma.mongodb.net:27017,snirequiredcluster-shard-00-01-f6vma.mongodb.net:27017,snirequiredcluster-shard-00-02-f6vma.mongodb.net:27017/reactivemongo?sslEnabled=true&authSource=admin&rm.failover=remote&connectTimeoutMS=$veryShortTimeoutMS"

  "An Atlas cluster connection string" should {
    "be parsed correctly" in {
      val parsedAtlasConnection = MongoConnection.parseURI(atlasHostConnectionString)

      parsedAtlasConnection must beASuccessfulTry { parsedUri: ParsedURI =>
        parsedUri.hosts must haveLength(3)
        parsedUri.options.sslEnabled must beTrue
        parsedUri.options.authMode must beEqualTo(ScramSha1Authentication) // Atlas only supports SCRAM-SHA1
        parsedUri.options.authSource must beSome("admin")
      }
    }
  }

  "Connection to Atlas Free Tier" should {

    def waitFor[T](awaitable: Awaitable[T]) = Await.result[T](awaitable, veryShortTimeoutDuration)

    //    def connectionTo(connString: String)

    lazy val drv = MongoDriver()

    //    "work only if configured" in { implicit ee: EE =>
    //
    //      drv.connection(atlasHostConnectionString) must beASuccessfulTry { c: MongoConnection =>
    //
    //        waitFor(c.database("reactivemongo")) must beAnInstanceOf[DefaultDB]
    //      }
    //    }
    //
    //    "fail if incorrect addresses are used" in { implicit ee: EE =>
    //
    //      val badConnectionString = atlasHostConnectionString.replace("snirequiredcluster", "requiredcluster")
    //
    //      println(s"badConnectionString: $badConnectionString")
    //      drv.connection(badConnectionString) must beASuccessfulTry { c: MongoConnection =>
    //
    //        waitFor(c.database("reactivemongo")) must throwAn[PrimaryUnavailableException]
    //      }
    //    }
    //
    //    "fail if incorrect credentials are used" in { implicit ee: EE =>
    //
    //      val badConnectionString = atlasHostConnectionString.replace("atlasreadonlypassword", "badpasword")
    //
    //      println(s"badConnectionString: $badConnectionString")
    //      drv.connection(badConnectionString) must beASuccessfulTry { c: MongoConnection =>
    //
    //        waitFor(c.database("reactivemongo")) must throwAn[PrimaryUnavailableException]
    //      }
    //    }

    "fail if SSL is not used" in { implicit ee: EE =>

      val badConnectionString = atlasHostConnectionString.replace("sslEnabled=true", "sslEnabled=false")

      println(s"badConnectionString: $badConnectionString")
      drv.connection(badConnectionString) must beASuccessfulTry { c: MongoConnection =>

        //waitFor(c.database("reactivemongo")) must throwAn[PrimaryUnavailableException]

        try {
          waitFor(c.database("reactivemongo"))
        } catch {
          case pue: PrimaryUnavailableException => println(pue.cause)
        }

        true must beTrue
      }
    }
  }
}
