/*
 * This example can be run from a `mvn scala:console` REPL.  Most of this
 * setup would get stashed away in your project and not have to be replayed
 * every time.  See also src/test/scala for an example setup.
 */
import scala.collection.JavaConverters._

import com.netflix.astyanax.connectionpool.Host
import com.netflix.astyanax.Cluster
import com.netflix.astyanax.serializers.ComparatorType

import com.spotright.polidoro.model._
import com.spotright.polidoro.serialization._
import com.spotright.polidoro.session._
import com.spotright.polidoro.testing.ClusterLoader
import com.spotright.polidoro.testing.keyspaces._


/** A "schema" object for testing. */
object DemoKS extends KeyspaceLoader {

  import ComparatorType._

  final val keyspaceName = "Demo"

  cdefs += ColumnFamilyDef("Users", keyType = UTF8TYPE.tyn, nameType = UTF8TYPE.tyn)
  cdefs += ColumnFamilyDef("Cities", nameType = "(UTF8Type, UTF8Type)")
}

/** Declare the keyspaces to be created during testing. */
object DemoLoader extends ClusterLoader {

  val keyspaces = List(
    DemoKS
  )
}

/** Declare the column families you'll be accessing (for Testing or Production). */
class DemoCFs(cluster: Cluster) {

  val ksDemo = cluster.getKeyspace("Demo").ensuring(_ != null, "null keyspace Demo$")

  val cfUsers = ColFam(ksDemo, "Users")    // keys=UTF8Type, names=UTF8Type

  val scfCities = ColFam(ksDemo, "Cities", hasCompositeName = true)
}


/** Cake up the cassandra connection object. */
object Cass extends TestableCassConnector with BatchOps {

  // In production you would probably load your seeds from a config file.
  val cass_seeds = List(new Host("localhost", 9160))

  protected var proxy: ContextContainer = new ContextContainerImpl(ContextContainerConfig("Demonstration", cass_seeds))
  protected val clusterLoader: ClusterLoader = DemoLoader

  object DemoFG extends DemoCFs(cluster)
}

// For this demo we want to create the Cluster and Keyspaces.
// Note that we'll also start an EmbeddedCassandra and not end up using the seeds above.
val _ = Cass.specTestInit()
println(s"Cass running on port: ${Cass.testPort}")

import Cass.DemoFG._
import com.spotright.polidoro.model.{CompositeFactory => CF}

// Insert some data.
Cass.batch(
  List(
    Insert(cfUsers \ "1234" \ ("name", "lanny")),
    Insert(cfUsers \ "1234" \ ("password", "theripple")),

    Insert( scfCities \ "US" \ (CF("TX", "Arlington"), "America/Chicago")),
    Insert( scfCities \ "US" \ (CF("TX", "Atascocita"), "America/Chicago")),
    Insert( scfCities \ "US" \ (CF("TX", "Austin"), "America/Chicago")),
    Insert( scfCities \ "US" \ (CF("TX", "Balch Springs"), "America/Chicago")),
    Insert( scfCities \ "US" \ (CF("TX", "Bay City"), "America/Chicago")),
    Insert( scfCities \ "US" \ (CF("TX", "Canyon Lake"), "America/Chicago"))
    )
)

// [default@DEMO] get Users['1234'];

// Get all columns in the row.
(cfUsers \ "1234").list[String].iterator.asScala.foreach {
  col =>
    println(s"=>  (column=${col.getName}, value=${col.getStringValue}, timestamp=${col.getTimestamp})")
}

// [default@DEMO] get Users['1234']['name'];

// Get a particular column if it exists.
(cfUsers \ "1234" \ "name").get.foreach {
  col =>
    println(s"=>  (column=${col.getName}, value=${col.getStringValue}, timestamp=${col.getTimestamp})")
}

// Composite column example.
val cities =
  (scfCities \ "US") listWithRange CompStr2.serdes using {
    _.withPrefix("TX")
      .greaterThanEquals("B")
      .lessThan("B".incr)
  }
cities.iterator.asScala.foreach {
  col =>
    val name = col.getName
    println(s"=>  (column=(${name.c1}, ${name.c2}), value=${col.getStringValue}")
}
