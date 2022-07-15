package io.github.scottweaver.zillen.netty

import io.netty.channel.epoll._
import io.netty.channel.kqueue._
import io.netty.bootstrap.Bootstrap
import zio._
import io.netty.util.concurrent.DefaultThreadFactory
import io.netty.channel._
import io.netty.handler.codec.http._
import io.netty.handler.logging.LoggingHandler
import io.netty.channel.unix.DomainSocketAddress
import io.netty.channel.socket.DuplexChannel
import zio.stream.ZSink

/** Inspirations and Due Respects:
  *   - https://github.com/slandelle/netty-progress/blob/master/src/test/java/slandelle/ProgressTest.java
  *   - https://github.com/dream11/zio-http
  *   - https://github.com/docker-java/docker-java
  */
object NettyRequest {

  def executeRequest(request: HttpRequest) =
    ZIO.serviceWithZIO[Channel] { channel =>
      val chunkHandler = new DockerResponseHandler()

      val schedule: Schedule[Any, Any, Any] =
        Schedule.recurWhile(_ => chunkHandler.httpStatus.get() == Integer.MIN_VALUE)

      val response = zhttp.service.ChannelFuture.make {

        channel
          .pipeline()
          .addLast(chunkHandler)

        channel.writeAndFlush(request)
      }
        .flatMap(_.toZIO)
        .flatMap(_ => ZIO.unit.schedule(schedule))
        .as(chunkHandler.httpStatus.get())

      val runningStream = chunkHandler.stream.run(ZSink.foreach(ZIO.debug(_)))

      for {
        f1         <- response.fork
        f2         <- runningStream.fork
        statusCode <- f1.join
        _          <- ZIO.debug("STATUS CODE JOINED")
        _          <- f2.join
        _          <- ZIO.debug("STREAM JOINED")
      } yield statusCode

    }

  def live                                                                                      = ZLayer.fromZIO {
    for {
      _       <- makeEventLoopGroup
      channel <- makeChannel()

    } yield channel
  }

  private def channelInitializer[A <: Channel]()                                                =
    new ChannelInitializer[A] {
      override def initChannel(ch: A): Unit = {
        ch.pipeline()
          .addLast(
            new LoggingHandler(getClass()),
            new HttpClientCodec(),
            new HttpContentDecompressor()
            // new HttpObjectAggregator(Int.MaxValue)
          )

        ()
      }
    }

  private def makeKqueue(bootstrap: Bootstrap)                                                  = ZIO.acquireRelease(
    ZIO.attempt {
      val evlg = new KQueueEventLoopGroup(0, new DefaultThreadFactory("zio-zillen-kqueue"))

      bootstrap
        .group(evlg)
        .channel(classOf[KQueueDomainSocketChannel])
        .handler(channelInitializer[KQueueDomainSocketChannel]())
      evlg
    }
  )(evlg => ZIO.attemptBlocking(evlg.shutdownGracefully().get()).ignoreLogged)

  private def makeEpoll(bootstrap: Bootstrap)                                                   = ZIO.acquireRelease(
    ZIO.attempt {
      val evlg = new EpollEventLoopGroup(0, new DefaultThreadFactory("zio-zillen-epoll"))

      val channelFactory: ChannelFactory[EpollDomainSocketChannel] = () => new EpollDomainSocketChannel()

      bootstrap
        .group(evlg)
        .channelFactory(channelFactory)
        .handler(channelInitializer[EpollDomainSocketChannel]())
      evlg
    }
  )(evlg => ZIO.attemptBlocking(evlg.shutdownGracefully().get()).ignoreLogged)

  private def makeChannel(path: String = "/var/run/docker.sock"): RIO[Bootstrap, DuplexChannel] = {
    val channel =
      ZIO.serviceWithZIO[Bootstrap] { bootstrap =>
        ZIO.attempt {
          bootstrap.connect(new DomainSocketAddress(path)).sync().channel()
        }
      }

    channel.flatMap {
      case c: DuplexChannel => ZIO.succeed(c)
      case other            => ZIO.fail(new Exception(s"Expected a duplex channel, got ${other.getClass.getName} instead."))
    }
  }

  private def makeEventLoopGroup: ZIO[Scope with Bootstrap, Throwable, EventLoopGroup]          =
    ZIO.serviceWithZIO[Bootstrap] { bootstrap =>
      if (Epoll.isAvailable())
        makeEpoll(bootstrap)
      else if (KQueue.isAvailable())
        makeKqueue(bootstrap)
      else
        ZIO.fail(new Exception("Could not create the appropriate event loop group.  Reason: OS not supported."))
    }

}
