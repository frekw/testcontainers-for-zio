/*
 * Copyright 2021 io.github.scottweaver
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.scottweaver.zio.testcontainers.spanner

import io.github.scottweaver.models.JdbcInfo
import io.github.scottweaver.zillen._
import zio._

import java.sql._
import javax.sql.DataSource
import com.google.cloud.spanner.SpannerOptions
import com.google.cloud.spanner.InstanceConfigInfo
import com.google.cloud.spanner.InstanceConfigId

final class SpannerContainer
    extends ContainerBootstrap[SpannerContainer.RIn, SpannerContainer.ROut, SpannerContainer](
      containerName = models.ContainerName("zio-spanner-test-container"),
      exposedPorts = Docker.makeExposedTCPPorts(5432),
      makeImage = ZIO.serviceWithZIO[SpannerContainer.Settings](settings =>
        Docker.makeImageZIO(s"gcr.io/cloud-spanner-emulator/emulator")
      ),
      makeEnv = ZIO.serviceWith[SpannerContainer.Settings](_.toEnv)
    ) {

  private def check(conn: Connection) =
    (ZIO.attemptBlocking {
      val stmt = conn.createStatement()
      val rs   = stmt.executeQuery("SELECT 1")
      rs.next()
      rs.getInt(1)
    }.mapError(
      Docker.invalidConfig(s"Failed to execute query against Spanner.")
    ).flatMap { i =>
      if (i == 1)
        ZIO.succeed(true)
      else
        Docker.failReadyCheckFailed(s"Spanner query check failed, expected 1, got $i")
    }).catchAll { case _ =>
      ZIO.succeed(false)
    }

  private def makeUrl(portMap: PortMap, settings: SpannerContainer.Settings) = {
    val hostInterface = portMap.findExternalHostPort(5432, Docker.protocol.TCP)
    val instanceName  = settings.instanceId
    val databaseName  = settings.database
    hostInterface match {
      case Some(hostInterface) =>
        ZIO.succeed(
          s"jdbc:cloudspanner://${hostInterface.hostAddress}/projects/project-id/instances/${instanceName}/databases/${databaseName};usePlainText=true"
        )
      case None =>
        Docker.failInvalidConfig(
          s"No listening host port found for Spanner container for internal port '5432/tcp'."
        )
    }
  }

  private def makeConnection(url: String) = {
    val acquireConn = ZIO
      .attempt(DriverManager.getConnection(url))
    val releaseConn = (conn: Connection) => ZIO.attempt(conn.close()).ignoreLogged
    ZIO
      .acquireRelease(acquireConn)(releaseConn)
      .tapError(t => ZIO.logWarning(s"Failed to establish a connection to Spanner @ '$url'. Cause: ${t.getMessage})"))
  }

  private def makeInstance(container: Container, settings: SpannerContainer.Settings) =
    for {
      hostPort <- ZIO
                    .fromOption(
                      container.networkSettings.ports.findExternalHostPort(9010, Docker.protocol.TCP)
                    )
                    .orElseFail(
                      Docker.invalidConfig(
                        s"No listening host port found for Spanner container for internal port '9010/tcp'.",
                        None
                      )
                    )
                    .orDieWith(e => new Throwable(e.toString))
      _ <-
        ZIO.attemptBlocking {
          // TODO: local connection?
          val options       = SpannerOptions.newBuilder().setEmulatorHost(hostPort.hostAddress).build()
          val spannerClient = options.getService()
          val instanceAdmin = spannerClient.getInstanceAdminClient()
          val dbAdminClient = spannerClient.getDatabaseAdminClient()

          val baseConfig = instanceAdmin.getInstanceConfig("emulator-config")
          val configInfo = InstanceConfigInfo
            .newBuilder(
              InstanceConfigId.of(settings.instanceId, settings.database),
              baseConfig
            )
            .setDisplayName(settings.instanceId)
            .build()

          import scala.jdk.CollectionConverters._

          instanceAdmin.createInstanceConfig(configInfo).get()
          dbAdminClient.createDatabase("instance-id", "database-id", List.empty[String].asJava).get()
          ()
        }.as(true).orDie
    } yield ()

  def readyCheck(container: Container): RIO[SpannerContainer.Settings with Scope, Boolean] =
    for {
      settings <- ZIO.service[SpannerContainer.Settings]
      url      <- makeUrl(container.networkSettings.ports, settings).mapError(_.asException)
      _        <- makeInstance(container, settings)
      conn     <- makeConnection(url)
      result   <- check(conn)
    } yield result

  def makeZEnvironment(
    container: Container
  ): ZIO[SpannerContainer.Settings with Scope, Nothing, ZEnvironment[SpannerContainer.ROut]] =
    (for {
      settings        <- ZIO.service[SpannerContainer.Settings]
      url             <- makeUrl(container.networkSettings.ports, settings).mapError(_.asException)
      conn            <- makeConnection(url)
      driverClassName <- ZIO.attempt(DriverManager.getDriver(url).getClass.getName)
    } yield {
      val jdbcInfo   = JdbcInfo(driverClassName, url, "", "")
      val dataSource = new com.google.cloud.spanner.jdbc.JdbcDataSource()
      dataSource.setUrl(url)
      // dataSource.setUser(jdbcInfo.username)
      // dataSource.setPassword(jdbcInfo.password)

      ZEnvironment(jdbcInfo, dataSource, conn, container)
    }).orDie

}

object SpannerContainer {
  final case class Settings(
    instanceId: String,
    database: String,
    additionalEnv: Env
  ) {

    private[spanner] def toEnv: Env =
      Docker
        .makeEnv(
          "SPANNER_INSTANCE" -> instanceId,
          "SPANNER_DATABASE" -> database
        ) && additionalEnv

    private[spanner] def toImage = Docker.makeImageZIO(s"gcr.io/cloud-spanner-emulator/emulator")
  }

  object Settings {
    def default(
      builder: Settings => Settings = identity,
      containerSettingsBuilder: ContainerSettings[SpannerContainer] => ContainerSettings[SpannerContainer] = identity
    ) = ZLayer.succeed {
      builder(
        Settings(
          instanceId = "zio-testcontainers",
          database = "zio-testcontainers",
          additionalEnv = Docker.makeEnv()
        )
      )
    } ++ ContainerSettings.default[SpannerContainer](containerSettingsBuilder)
  }

  type ROut = JdbcInfo with DataSource with Connection with Container

  type RIn = Settings with Scope

  val layer = (new SpannerContainer).layer

}
