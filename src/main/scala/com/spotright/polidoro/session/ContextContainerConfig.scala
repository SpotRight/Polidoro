package com.spotright.polidoro.session

import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor
import com.netflix.astyanax.connectionpool.{Host,ConnectionPoolMonitor, ConnectionPoolConfiguration}
import com.netflix.astyanax.model.ConsistencyLevel
import com.netflix.astyanax.thrift.ThriftFamilyFactory
import com.netflix.astyanax.{AstyanaxTypeFactory, AstyanaxConfiguration}

import org.apache.cassandra.thrift.Cassandra

/**
 * Configuration information for a [[com.spotright.polidoro.session.ContextContainerImpl]]
 *
 * @param clusterName The name of the cluster
 * @param seedHosts Hosts used to fetch ring information
 * @param defaultReadCL The default ConsistencyLevel for reads
 * @param defaultWriteCL The default ConsitencyLevel for writes
 * @param configDecorator A function which can modify the AstyanaxConfiguration used to construct the AstytanaxContext
 * @param connectionPoolDecorator A function which can modify the ConnectionPoolConfiguration used to construct the AstytanaxContext
 * @param connectionPoolMonitor A factory for an instance of the ConnectionPoolMonitor used to construct the AstytanaxContext
 * @param typeFactory A factory for an instance of the AstyanaxTypeFactory used to construct the AstyanaxContext
 */
case class ContextContainerConfig(
  clusterName: String,
  seedHosts: Seq[Host],
  defaultReadCL: ConsistencyLevel = ConsistencyLevel.CL_QUORUM,
  defaultWriteCL: ConsistencyLevel = ConsistencyLevel.CL_QUORUM,
  configDecorator: AstyanaxConfiguration => AstyanaxConfiguration = identity,
  connectionPoolDecorator: ConnectionPoolConfiguration => ConnectionPoolConfiguration = identity,
  connectionPoolMonitor: () => ConnectionPoolMonitor = () => new CountingConnectionPoolMonitor(),
  typeFactory: () => AstyanaxTypeFactory[Cassandra.Client] = () => ThriftFamilyFactory.getInstance()
                                   ) {
  require(clusterName != null, "clusterName is null")
  require(!clusterName.isEmpty, "clusterName is empty")
  require(!seedHosts.isEmpty, "empty seedHosts")
}
