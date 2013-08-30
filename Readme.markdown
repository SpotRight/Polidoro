# Polidoro

Polidoro was the son of Astyanax.

As far as this repo goes Polidoro is a Cassandra client written in
Scala.  It is a light sugar over [Astyanax](https://github.com/Netflix/astyanax)
in the style of [Cascal](https://github.com/shorrockin/cascal).

# Features

* Simple data model with support for Composite columns
* Direct access to Astyanax for complex tasks

# Overview of Use

To use Polidoro you create a class or classes that model your family
groups (those column families in the same keyspace).  Family groups
are contained in a `CassConnector` which holds the Astyanax context.
From there you create your data columns as needed for insert or
delete.  You can use a `TestableCassConnector` to set up your connection
for spawning a daemonized Cassandra instance for testing with
[specs2](http://etorreborre.github.com/specs2/).

The tests set up in the repo demonstrate a connector (`SpotAsty`)
with a family group (`DemonstrationFG`) along with the setup needed
for testing (`csh.testing.DemoClusterLoader`, `csh.testing.keyspaces.Demonstration`)

# Data Model

(Use `import com.spotright.polidoro.model._` to enable code below.)

Polidoro uses the data model put forward by Cascal.  A column path is
created using the '\' operator.

    val col = cfPeople \ "Lanny Ripple" \ ("state", "TX")

Composite constructs are supported at the key and columnname level

    import CompositeFactory.CF
    val col = scfUsers \ "Ripple" (CF("Lanny", "state"), "TX")

Once you have enough of a column path to operate on you can call
various methods to get, list, or mutate.  Getting a value (returning
an `Option[cna.model.Column[N]]`) and listing (returning a
`cna.model.ColumnList[N]`) are straightforward

    val maybeCol = (cfPeople \ "Lanny Ripple" \ "state").get

and

    import com.spotright.polidoro.serialization._
    val cols = (scfUsers \ "Ripple") listWith CompStr2.serdes

As an aside Astyanax cares about the key, column name, and value types.
These have consistently been named K, N, and V respectively in the
Polidoro code.

For mutation the simplest way is to use `csh.model` case classes that
descend from `Operation` and then batch the results.

    val ops = List(
      Insert(cfPeople \ "Dave Angulo" \ ("state", "CO"),
      Delete(cfPeople \ "Dave Angulo" \ "status"),
      IncrCounter(cfCounters \ "typos" \ ("colnames", 1L)),
      IncrCounter(cfCounters \ "typos" \ ("keys", 1L))
    )

    SpotAsty.batch(ops)

Mutation is supported directly on columns (see `csh.model.ColumnPath`)
but in practice we find it simpler to batch().
