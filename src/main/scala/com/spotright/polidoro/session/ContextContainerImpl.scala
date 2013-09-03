package com.spotright.polidoro.session

import scala.collection.JavaConverters._

import com.spotright.polidoro.util.LogHelper

class ContextContainerImpl(ccc: ContextContainerConfig) extends ContextContainer with LogHelper {

  import com.netflix.astyanax.connectionpool.NodeDiscoveryType
  import com.netflix.astyanax.connectionpool.impl._
  import com.netflix.astyanax.impl.AstyanaxConfigurationImpl
  import com.netflix.astyanax.{Cluster, AstyanaxContext}

  val astyConfig =
    new AstyanaxConfigurationImpl()
      .setCqlVersion("3.0.0")
      .setTargetCassandraVersion("1.2")
      .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
      .setConnectionPoolType(ConnectionPoolType.TOKEN_AWARE)
      .setDefaultReadConsistencyLevel(ccc.defaultReadCL)
      .setDefaultWriteConsistencyLevel(ccc.defaultWriteCL)

  val astyConnPool =
    new ConnectionPoolConfigurationImpl(ccc.clusterName)
      .setMaxConns(60)
      .setMaxConnsPerHost(12)
      .setConnectTimeout(10 * 1000)
      .setSeeds(ccc.seedHosts.map{_.getUrl}.mkString(","))

  lazy val context: AstyanaxContext[Cluster] = {
    logger.debug("Building AstyanaxContext for cluster \"{}\"", ccc.clusterName)

    val ctx = new AstyanaxContext.Builder()
      .forCluster(ccc.clusterName)
      .withAstyanaxConfiguration(ccc.configDecorator(astyConfig))
      .withConnectionPoolConfiguration(ccc.connectionPoolDecorator(astyConnPool))
      .withConnectionPoolMonitor(ccc.connectionPoolMonitor())
      .buildCluster(ccc.typeFactory())

    val cphosts = ctx.getConnectionPool.getPools.asScala.map {_.getHost.getName}
    logger.debug("ConnectionPool hosts = {}", cphosts.mkString(", "))

    ctx
  }
}
