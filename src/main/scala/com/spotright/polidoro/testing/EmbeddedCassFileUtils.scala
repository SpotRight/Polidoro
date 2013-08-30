/*
 * ******************************************************************************
 *    Copyright 2012-2013 SpotRight
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 * ******************************************************************************
 */

package com.spotright.polidoro
package testing

import scala.io.Source

import java.io.{IOException, FileOutputStream, File}

import org.apache.cassandra.config.DatabaseDescriptor
import org.apache.cassandra.db.commitlog.CommitLog
import org.apache.cassandra.io.util.FileUtils

trait EmbeddedCassFileUtils {
  self: EmbeddedCass =>

  import EmbeddedCass._

  def copyAndModifyYamlFile(yamlFile: String, rtdir: String, rtConfname: String): Int = {
    val thriftPort = generatePort

    val DataFileDirectoriesLine = """^(data_file_directories:)""".qr
    val DataFileDirectoryLine = """^(\s*- )""".qr
    val CommitLogDirLine = """^(commitlog_directory:\s)""".qr
    val SavedCachesDirLine = """^(saved_caches_directory:\s)""".qr
    val StoragePortLine = """^(storage_port:\s)""".qr
    val RpcPortLine = """^(rpc_port:\s)""".qr

    val home = new File(rtdir).getCanonicalPath()
    val fname = home + "/" + rtConfname
    val out = new java.io.FileWriter(fname)

    var gate: Gate.Step = Gate.Seeking
    def srep(pfx: String, payload: String = ""): String =  pfx + home + "/" + payload + "\n"

    for (line <- Source.fromInputStream(getClass.getResourceAsStream(yamlFile)).getLines()) {
      gate = gate match {
        case Gate.Seen => Gate.Expect
        case Gate.Expect => Gate.Seeking
        case other => other
      }

      val outLine = line match {
        case DataFileDirectoriesLine(dfd) =>
          gate = Gate.Seen
          dfd + "\n"
        case DataFileDirectoryLine(blanks) if gate == Gate.Expect =>
          gate = Gate.Done
          srep(blanks, "data")
        case CommitLogDirLine(cl) => srep(cl, "commitlog")
        case SavedCachesDirLine(sc) => srep(sc, "saved_caches")
        case StoragePortLine(sp) => sp + generatePort + "\n"
        case RpcPortLine(rp) => rp + thriftPort + "\n"
        case unmod => unmod + "\n"
      }
      out.write(outLine)
    }
    out.close

    thriftPort
  }

  val random = new scala.util.Random

  // scala.actor.remote.TcpService.generatePort
  def generatePort: Int = {
    var portnum = 0
    try {
      portnum = 8000 + random.nextInt(500)
      val socket = new java.net.ServerSocket(portnum)
      socket.close()
    }
    catch {
      case ioe: IOException => generatePort
      case se: SecurityException =>
    }
    portnum
  }

  /**
   * Copies a resource from within the jar to a directory.
   *
   * @param resource
   * @param directory
   * @throws IOException
   */
  protected
  def copy(resource: String, directory: String) {
    mkdir(directory)
    val is = this.getClass.getResourceAsStream(resource)
    val fileName = resource.substring(resource.lastIndexOf("/") + 1)
    val file = new File(directory + System.getProperty("file.separator") + fileName)
    val out = new FileOutputStream(file)
    val buf = new Array[Byte](1024)

    var len = is.read(buf)
    while (len > 0) {
      out.write(buf, 0, len)
      len = is.read(buf)
    }

    out.close()
    is.close()
  }

  protected
  def cleanupAndLeaveDirs() {
    mkdirs()
    cleanup()
    mkdirs()
    CommitLog.instance.resetUnsafe() // cleanup screws w/ CommitLog, this brings it back to safe state
  }

  protected
  def cleanup() {
    // clean up commitlog
    val directoryNames = Array(DatabaseDescriptor.getCommitLogLocation)

    for (dirName <- directoryNames) {
      val dir = new File(dirName)
      if (!dir.exists())
        throw new RuntimeException("No such directory: " + dir.getAbsolutePath())

      FileUtils.deleteRecursive(dir)
    }

    // clean up data directory which are stored as data directory/table/data files
    for (dirName <- DatabaseDescriptor.getAllDataFileLocations())
    {
      val dir = new File(dirName)
      if (!dir.exists())
        throw new RuntimeException("No such directory: " + dir.getAbsolutePath())

      FileUtils.deleteRecursive(dir)
    }
  }

  /**
   * Creates a directory
   *
   * @param dir
   * @throws IOException
   */
  protected
  def mkdir(dir: String) {
    FileUtils.createDirectory(dir)
  }

  protected
  def mkdirs() {
    try DatabaseDescriptor.createAllDirectories()
    catch {
      case e: IOException => throw new RuntimeException(e)
    }
  }

  protected
  def rmdir(dir: String) {
    val dirFile = new File(dir)
    if (dirFile.exists()) {
      FileUtils.deleteRecursive(dirFile)
    }
  }
}


