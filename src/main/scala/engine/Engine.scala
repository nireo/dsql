package engine

import java.sql.Connection
import java.sql.DriverManager
import scala.util.Try
import scala.util.Failure
import scala.util.Using
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import java.sql.ResultSet
import scala.collection.mutable.ArrayBuffer

class Engine(path: String):
  private val config = new HikariConfig()
  config.setJdbcUrl(s"jdbc:sqlite:$path")
  config.setPoolName("sqlite-pool")

  config.setMaximumPoolSize(1)
  config.setAutoCommit(false)

  config.setConnectionTimeout(30000) // 30 seconds
  config.setIdleTimeout(600000) // 10 minutes
  config.setMaxLifetime(1800000) // 30 minutes

  private val dsrc = new HikariDataSource(config)

  def withConnection[T](op: Connection => T): Try[T] =
    for
      conn <- Try(dsrc.getConnection())
      result <- Try(op(conn))
        .map { r =>
          conn.commit()
          conn.close()
          r
        }
        .recoverWith { case e =>
          conn.rollback()
          conn.close()
          Failure(e)
        }
    yield result
  end withConnection

  def execute(sql: String): Try[Boolean] =
    withConnection { conn =>
      val stmt = conn.createStatement()
      stmt.execute(sql)
    }
  end execute

  def query[T](sql: String)(mapper: ResultSet => T): Try[Vector[T]] =
    withConnection { conn =>
      val stmt = conn.createStatement()
      val rs = stmt.executeQuery(sql)
      val buffer = ArrayBuffer[T]()
      while rs.next() do buffer += mapper(rs)

      buffer.toVector
    }
  end query

end Engine
