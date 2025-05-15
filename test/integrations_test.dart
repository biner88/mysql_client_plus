import 'dart:typed_data';
import 'package:mysql_client_plus/mysql_client_plus.dart';
import 'package:test/test.dart';

void main() {
  MySQLConnection? conn;

  setUpAll(() async {
    // Create a connection with the bank
    conn = await MySQLConnection.createConnection(
      host: 'localhost',
      port: 3306,
      userName: 'your_user',
      password: 'your_password',
      databaseName: 'testdb',
      secure: true,
    );
    await conn!.connect();

    // Create a table with different columns for the types
    await conn!.execute('DROP TABLE IF EXISTS my_table');
    await conn!.execute('''
      CREATE TABLE my_table (
        id INT AUTO_INCREMENT PRIMARY KEY,
        int_column INT,
        string_column VARCHAR(255),
        datetime_column DATETIME,
        blob_column BLOB,
        bool_column TINYINT(1),
        decimal_column DECIMAL(10,2),
        float_column FLOAT,
        double_column DOUBLE,
        date_column DATE,
        time_column TIME,
        year_column YEAR,
        json_column JSON
      )
    ''');
  });

  tearDownAll(() async {
    try {
      await conn!.close();
    } catch (e) {
      // Log or ignore if the connection is not dated
    }
  });

  test("Inserindo um inteiro via prepared statement", () async {
    final stmt =
        await conn!.prepare('INSERT INTO my_table (int_column) VALUES (?)');
    final result = await stmt.execute([42]);
    expect(result.affectedRows.toInt(), equals(1));
    await stmt.deallocate();
  });

  test("Inserindo uma string via prepared statement", () async {
    final stmt =
        await conn!.prepare('INSERT INTO my_table (string_column) VALUES (?)');
    final result = await stmt.execute(['Hello, world!']);
    expect(result.affectedRows.toInt(), equals(1));
    await stmt.deallocate();
  });

  test("Inserindo um DateTime via prepared statement", () async {
    final now = DateTime.now();
    final stmt = await conn!
        .prepare('INSERT INTO my_table (datetime_column) VALUES (?)');
    final result = await stmt.execute([now]);
    expect(result.affectedRows.toInt(), equals(1));
    await stmt.deallocate();
  });

  test("Inserting binary data (Uint8List) via prepared statement", () async {
    final stmt =
        await conn!.prepare('INSERT INTO my_table (blob_column) VALUES (?)');
    // Represents "Hello"
    final myBytes = Uint8List.fromList([0x48, 0x65, 0x6c, 0x6c, 0x6f]);
    final result = await stmt.execute([myBytes]);
    expect(result.affectedRows.toInt(), equals(1));
    await stmt.deallocate();
  });

  test("Inserindo um booleano via prepared statement", () async {
    final stmt =
        await conn!.prepare('INSERT INTO my_table (bool_column) VALUES (?)');
    final result = await stmt.execute([true]);
    expect(result.affectedRows.toInt(), equals(1));
    await stmt.deallocate();
  });

  test("Inserindo um valor DECIMAL via prepared statement", () async {
    final stmt =
        await conn!.prepare('INSERT INTO my_table (decimal_column) VALUES (?)');
    // We use a number that can be converted to String or num;
    // in this example, 1234.56
    final result = await stmt.execute([1234.56]);
    expect(result.affectedRows.toInt(), equals(1));
    await stmt.deallocate();
  });

  test("Inserindo um valor FLOAT via prepared statement", () async {
    final stmt =
        await conn!.prepare('INSERT INTO my_table (float_column) VALUES (?)');
    final result = await stmt.execute([3.14]);
    expect(result.affectedRows.toInt(), equals(1));
    await stmt.deallocate();
  });

  test("Inserindo um valor DOUBLE via prepared statement", () async {
    final stmt =
        await conn!.prepare('INSERT INTO my_table (double_column) VALUES (?)');
    final result = await stmt.execute([2.718281828]);
    expect(result.affectedRows.toInt(), equals(1));
    await stmt.deallocate();
  });

  test("Inserindo uma DATA via prepared statement", () async {
    final stmt =
        await conn!.prepare('INSERT INTO my_table (date_column) VALUES (?)');
    final result = await stmt.execute(['2023-05-01']);
    expect(result.affectedRows.toInt(), equals(1));
    await stmt.deallocate();
  });

  test("Inserindo um TIME via prepared statement", () async {
    final stmt =
        await conn!.prepare('INSERT INTO my_table (time_column) VALUES (?)');
    // Using a non-format string "HH:MM:SS"
    final result = await stmt.execute(['15:30:45']);
    expect(result.affectedRows.toInt(), equals(1));
    await stmt.deallocate();
  });

  test("Inserindo um YEAR via prepared statement", () async {
    final stmt =
        await conn!.prepare('INSERT INTO my_table (year_column) VALUES (?)');
    // Using an integer or string representing the year
    final result = await stmt.execute([2023]);
    expect(result.affectedRows.toInt(), equals(1));
    await stmt.deallocate();
  });
  final dt = DateTime(2023, 6, 15, 10, 20, 30);
  final blobData = Uint8List.fromList([0x01, 0x02, 0x03]);

  test('Inserting and validating complete line', () async {
    // Enter the line and obtain the ID
    final stmtInsert = await conn!.prepare('''
      INSERT INTO my_table 
      (int_column, string_column, datetime_column, blob_column, bool_column, 
       decimal_column, float_column, double_column, date_column, time_column, year_column)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''');
    final insertResult = await stmtInsert.execute([
      123,
      'Test String',
      dt,
      blobData,
      false,
      99.99,
      1.23,
      4.56,
      '2023-06-15',
      '12:34:56',
      2023,
    ]);
    expect(insertResult.affectedRows.toInt(), equals(1));
    await stmtInsert.deallocate();

    final stmtId = await conn!.prepare('SELECT LAST_INSERT_ID() AS id');
    final idResult = await stmtId.execute([]);
    expect(idResult.numOfRows, greaterThan(0));
    final insertedId = int.tryParse(idResult.rows.first.colAt(0)!);
    await stmtId.deallocate();

    // Valida os valores inseridos
    final stmtSelect = await conn!.prepare('''
      SELECT int_column, string_column, datetime_column, blob_column, bool_column, 
             decimal_column, float_column, double_column, date_column, time_column, year_column
      FROM my_table
      WHERE id = ?
    ''');
    final selectResult = await stmtSelect.execute([insertedId]);
    expect(selectResult.numOfRows, greaterThan(0));
    final row = selectResult.rows.first;

    expect(row.colAt(0), equals('123'));
    expect(row.colAt(1), equals('Test String'));
    expect(row.typedColAt<DateTime>(2), equals(dt));
    expect(row.colAt(3), isNotNull);
    expect(row.colAt(4), equals('0'));
    expect(row.colAt(5), equals('99.99'));
    expect(double.parse(row.colAt(6)!), closeTo(1.23, 0.001));
    expect(double.parse(row.colAt(7)!), closeTo(4.56, 0.001));
    expect(row.colAt(8)?.substring(0, 10), equals('2023-06-15'));
    expect(row.colAt(9), startsWith('12:34:56'));
    expect(row.colAt(10), equals('2023'));

    await stmtSelect.deallocate();
  });

  test("Duplicate insertion must throw duplicate key error", () async {
    // Create (or recreate) a test table with primary key for the id
    await conn!.execute("DROP TABLE IF EXISTS test_dup");
    await conn!.execute('''
      CREATE TABLE test_dup (
        id INT PRIMARY KEY,
        name VARCHAR(50)
      )
    ''');

    // Insere a primeira linha com id = 1
    final stmtInsert =
        await conn!.prepare("INSERT INTO test_dup (id, name) VALUES (?, ?)");
    final result1 = await stmtInsert.execute([1, "Original"]);
    expect(result1.affectedRows.toInt(), equals(1));
    await stmtInsert.deallocate();

    // Try inserting another line with id = 1, or it should generate an error
    final stmtDup =
        await conn!.prepare("INSERT INTO test_dup (id, name) VALUES (?, ?)");
    try {
      await stmtDup.execute([1, "Duplicado"]);
      fail("Deveria lançar erro de chave duplicada");
    } catch (e) {
      // Verifica se a mensagem de erro contém "Duplicate entry"
      expect(e.toString(), contains("Duplicate entry"),
          reason:
              "Or the error should indicate that there is no entry with key '1'");
    }
    await stmtDup.deallocate();
  });
  test("Inserindo uma DATA via prepared statement", () async {
    final stmt =
        await conn!.prepare('INSERT INTO my_table (json_column) VALUES (?)');
    final result = await stmt.execute(['{"name":"Alice","age":30}']);
    expect(result.affectedRows.toInt(), equals(1));
    await stmt.deallocate();
  });
//end
}
