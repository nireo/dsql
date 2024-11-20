package engine

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach
import org.scalatest.BeforeAndAfterAll
import java.nio.file.{Files, Paths}
import scala.util.{Success, Failure}

class EngineSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterEach
    with BeforeAndAfterAll {
  private val dbPath = "test.db"
  private var engine: Engine = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    engine = new Engine(dbPath)

    // Create test tables with different data types
    engine
      .execute("""
      CREATE TABLE IF NOT EXISTS test_types (
        id INTEGER PRIMARY KEY,
        int_col INTEGER,
        real_col REAL,
        text_col TEXT,
        nullable_col INTEGER
      )
    """)
      .get

    engine
      .execute("""
      CREATE TABLE IF NOT EXISTS test_users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        age INTEGER,
        salary REAL
      )
    """)
      .get
  }

  override def afterAll(): Unit = {
    Files.deleteIfExists(Paths.get(dbPath))
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    engine.execute("DELETE FROM test_types").get
    engine.execute("DELETE FROM test_users").get
  }

  "Engine" should "handle different SQLite data types correctly" in {
    // Insert test data with different types
    engine
      .execute("""
      INSERT INTO test_types (int_col, real_col, text_col, nullable_col)
      VALUES (42, 3.14, 'hello', NULL)
    """)
      .get

    val result = engine.query("SELECT * FROM test_types")
    result shouldBe a[Success[_]]

    val queryResult = result.get
    queryResult.columns.map(
      _.name
    ) should contain allOf ("id", "int_col", "real_col", "text_col", "nullable_col")

    val row = queryResult.rows.head
    row(1) shouldBe a[LongValue]
    row(1).asString shouldBe "42"

    row(2) shouldBe a[DoubleValue]
    row(2).asString shouldBe "3.14"

    row(3) shouldBe a[StringValue]
    row(3).asString shouldBe "hello"

    row(4) shouldBe a[NullValue]
    row(4).asString shouldBe "null"
  }

  it should "return correct column metadata" in {
    val result = engine.query("SELECT * FROM test_types")
    result shouldBe a[Success[_]]

    val metadata = result.get.columns
    metadata.length shouldBe 5

    val idCol = metadata(0)
    idCol.name shouldBe "id"
    idCol.typeName shouldBe "INTEGER"
    idCol.isNullable shouldBe false // PRIMARY KEY

    val nullableCol = metadata(4)
    nullableCol.name shouldBe "nullable_col"
    nullableCol.typeName shouldBe "INTEGER"
    nullableCol.isNullable shouldBe true
  }

  it should "handle empty result sets" in {
    val result = engine.query("SELECT * FROM test_types WHERE int_col > 1000")
    result shouldBe a[Success[_]]

    val queryResult = result.get
    queryResult.columns.isEmpty shouldBe false // Should still have column metadata
    queryResult.rows.isEmpty shouldBe true // But no rows
  }

  it should "handle multiple rows" in {
    // Insert test data
    engine
      .execute("""
      INSERT INTO test_users (name, age, salary) VALUES 
      ('Alice', 30, 50000.0),
      ('Bob', 25, 45000.0),
      ('Charlie', 35, 60000.0)
    """)
      .get

    val result = engine.query("SELECT * FROM test_users ORDER BY age")
    result shouldBe a[Success[_]]

    val queryResult = result.get
    queryResult.rows.length shouldBe 3

    // Check second row (Bob)
    val bobRow = queryResult.rows(0)
    bobRow(1).asString shouldBe "Bob"
    bobRow(2).asString shouldBe "25"
    bobRow(3).asString shouldBe "45000.0"
  }

  it should "fail gracefully on invalid SQL" in {
    val result = engine.query("SELECT * FROM nonexistent_table")
    result shouldBe a[Failure[_]]
  }

  it should "handle transactions correctly" in {
    // First insert should succeed
    engine
      .execute("""
      INSERT INTO test_users (name, age, salary)
      VALUES ('Dave', 40, 70000.0)
    """)
      .get

    // This transaction should fail due to invalid SQL
    val failedResult = engine.withConnection { conn =>
      val stmt = conn.createStatement()
      stmt.execute(
        "INSERT INTO test_users (name, age, salary) VALUES ('Eve', 45, 75000.0)"
      )
      stmt.execute("INVALID SQL STATEMENT")
      true
    }

    failedResult shouldBe a[Failure[_]]

    // Verify only the first insert exists
    val queryResult = engine.query("SELECT COUNT(*) as count FROM test_users")
    queryResult shouldBe a[Success[_]]
    val count = queryResult.get.rows.head.head
    count.asString shouldBe "1"
  }

  it should "handle aggregate functions" in {
    // Insert test data
    engine
      .execute("""
      INSERT INTO test_users (name, age, salary) VALUES 
      ('Alice', 30, 50000.0),
      ('Bob', 25, 45000.0),
      ('Charlie', 35, 60000.0)
    """)
      .get

    val result = engine.query("""
      SELECT 
        COUNT(*) as count,
        AVG(age) as avg_age,
        MAX(salary) as max_salary,
        MIN(salary) as min_salary
      FROM test_users
    """)

    result shouldBe a[Success[_]]
    val queryResult = result.get
    val row = queryResult.rows.head

    row(0).asString shouldBe "3" // COUNT(*)
    row(1).asString shouldBe "30.0" // AVG(age)
    row(2).asString shouldBe "60000.0" // MAX(salary)
    row(3).asString shouldBe "45000.0" // MIN(salary)
  }

  it should "handle GROUP BY queries" in {
    // Insert test data with some duplicates
    engine
      .execute("""
      INSERT INTO test_users (name, age, salary) VALUES 
      ('Alice', 30, 50000.0),
      ('Bob', 30, 45000.0),
      ('Charlie', 35, 60000.0),
      ('Dave', 35, 65000.0)
    """)
      .get

    val result = engine.query("""
      SELECT age, COUNT(*) as count, AVG(salary) as avg_salary
      FROM test_users
      GROUP BY age
      ORDER BY age
    """)

    result shouldBe a[Success[_]]
    val queryResult = result.get
    queryResult.rows.length shouldBe 2 // Two age groups

    // Check first group (age 30)
    val group30 = queryResult.rows(0)
    group30(0).asString shouldBe "30"
    group30(1).asString shouldBe "2" // Count
    group30(2).asString shouldBe "47500.0" // Average salary
  }
}
