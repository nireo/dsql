package engine

import java.sql.Connection
import java.sql.DriverManager
import scala.util.Try
import scala.util.Failure
import scala.util.Using
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import scala.collection.mutable.ArrayBuffer

// Represents a column's metadata
case class ColumnMetadata(
    name: String,
    typeName: String,
    isNullable: Boolean
)

// Represents a single cell value that can be serialized
sealed trait ColumnValue:
  def asString: String

case class StringValue(value: String) extends ColumnValue:
  def asString: String = value

case class LongValue(value: Long) extends ColumnValue:
  def asString: String = value.toString

case class DoubleValue(value: Double) extends ColumnValue:
  def asString: String = value.toString

case class BooleanValue(value: Boolean) extends ColumnValue:
  def asString: String = value.toString

case class NullValue() extends ColumnValue:
  def asString: String = "null"

// Represents a complete result set that can be serialized
case class QueryResult(
    columns: Vector[ColumnMetadata],
    rows: Vector[Vector[ColumnValue]]
)

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

  private def getColumnMetadata(
      meta: ResultSetMetaData
  ): Vector[ColumnMetadata] =
    val cols = ArrayBuffer[ColumnMetadata]()
    for i <- 1 to meta.getColumnCount do
      cols += ColumnMetadata(
        name = meta.getColumnName(i),
        typeName = meta.getColumnTypeName(i),
        isNullable = meta.isNullable(i) == ResultSetMetaData.columnNullable
      )
    cols.toVector
  end getColumnMetadata

  private def getColumnValue(rs: ResultSet, columnIndex: Int): ColumnValue =
    val typeName = rs.getMetaData().getColumnTypeName(columnIndex).toUpperCase
    if rs.getObject(columnIndex) == null then return NullValue()

    typeName match
      case "INTEGER" => LongValue(rs.getLong(columnIndex))
      case "REAL"    => DoubleValue(rs.getDouble(columnIndex))
      case "BOOLEAN" => BooleanValue(rs.getBoolean(columnIndex))
      case "TEXT" | "VARCHAR" | "CHAR" => StringValue(rs.getString(columnIndex))
      case _ =>
        StringValue(rs.getString(columnIndex))
  end getColumnValue

  def query(sql: String): Try[QueryResult] =
    withConnection { conn =>
      val stmt = conn.createStatement()
      val rs = stmt.executeQuery(sql)
      val meta = rs.getMetaData()
      val columnCount = meta.getColumnCount()

      val columns = getColumnMetadata(meta)

      val rows = ArrayBuffer[Vector[ColumnValue]]()
      while rs.next() do
        val row = ArrayBuffer[ColumnValue]()
        for i <- 1 to columnCount do row += getColumnValue(rs, i)
        rows += row.toVector

      QueryResult(columns, rows.toVector)
    }
  end query
end Engine
