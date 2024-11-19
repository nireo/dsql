package engine

import java.sql.Connection
import java.sql.DriverManager
import scala.util.Try
import scala.util.Using
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

class Engine(path: String):
  private val config = new HikariCP
end Engine
