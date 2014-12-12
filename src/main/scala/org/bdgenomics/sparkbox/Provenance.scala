package org.bdgenomics.sparkbox

import java.net.InetAddress
import java.util.Calendar

import scala.collection.JavaConversions

object Provenance {

  private var arguments: Seq[String] = Seq.empty

  def setArguments(newArguments: Seq[String]) = {
    assume(arguments.isEmpty, "setArguments should only be called once.")
    arguments = newArguments
  }

  def get(attributes: (String, String)*): avro.Provenance = {
    val builder = avro.Provenance.newBuilder()
    builder.setHost(InetAddress.getLocalHost().getHostName())
    builder.setUser(System.getProperty("user.name"))
    builder.setTimestamp(Calendar.getInstance.getTime.toString)
    builder.setArguments(JavaConversions.seqAsJavaList(arguments))
    builder.setAttributes(JavaConversions.mapAsJavaMap(attributes.toMap))
    builder.build()
  }
}
