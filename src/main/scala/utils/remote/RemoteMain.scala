package utils.remote

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory

/** Remote main.
  * Lauch an actor system waiting for remote actor creation.
  */
object RemoteMain {

  def main(args: Array[String]): Unit = {
    val systemName = args(0)
    val ip = args(1)
    val port = args(2)
    val configIp = "akka.remote.netty.tcp.hostname = \"" + ip + "\""
    val configPort = "akka.remote.netty.tcp.port = \"" + port + "\""
    val config = ConfigFactory.parseString(configIp + "\n" + configPort)
    val system = ActorSystem(
      systemName,
      config.withFallback(ConfigFactory.load())
    )
  }

}
