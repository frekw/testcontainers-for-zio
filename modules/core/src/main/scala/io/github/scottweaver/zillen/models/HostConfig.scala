package io.github.scottweaver
package zillen
package models

import zio.json._

final case class HostConfig(
  @jsonField("PortBindings") portBindings: PortMap
)

object HostConfig {

  val empty = HostConfig(PortMap.empty)

  implicit val HostConfigCodec: JsonCodec[HostConfig] = DeriveJsonCodec.gen[HostConfig]

}
