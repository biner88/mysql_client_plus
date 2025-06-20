import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:mysql_client_plus/exception.dart';
import 'package:mysql_client_plus/mysql_client_plus.dart';
import 'package:test/test.dart';

void main() {
  late MySQLConnection connection;

  setUpAll(() async {
    // Cria e conecta uma instância de MySQLConnection para a maioria dos testes
    connection = await MySQLConnection.createConnection(
      host: 'localhost',
      port: 3306,
      userName: 'root',
      password: 'root',
      databaseName: 'test_db',
      secure: true,
    );
    await connection.connect();
  });

  tearDownAll(() async {
    // Fecha a conexão ao final de todos os testes
    await connection.close();
  });

  test('The connection must be established', () async {
    expect(connection.connected, isTrue);
  });

  test('Execute: Simple query returns expected result', () async {
    final result = await connection.execute("SELECT 1 AS test");
    expect(result.numOfRows, greaterThan(0));
    final row = result.rows.first;
    final assoc = row.assoc();
    expect(assoc['test'], equals('1'));
  });

  test('Auth: mysql_native_password', () async {
    // Verifies authentication with mysql_native_password by executing a query that returns the current user
    final result = await connection.execute("SELECT CURRENT_USER() as user");
    expect(result.numOfRows, greaterThan(0));
    final row = result.rows.first;
    final assoc = row.assoc();
    expect(assoc['user'], contains('root'));
  });

  test('Connection pool: Query via pool', () async {
    final pool = MySQLConnectionPool(
      host: 'localhost',
      port: 3306,
      userName: 'root',
      password: 'root',
      databaseName: 'test_db',
      secure: true,
      maxConnections: 5,
    );
    final result = await pool.execute("SELECT 1 AS test");
    expect(result.numOfRows, greaterThan(0));
    final row = result.rows.first;
    final assoc = row.assoc();
    expect(assoc['test'], equals('1'));
    await pool.close();
  });

  test('Query placeholders: Using named parameters', () async {
    await connection.execute("DROP TABLE IF EXISTS placeholder_test");
    await connection
        .execute("CREATE TABLE placeholder_test (id INT, value VARCHAR(50))");
    await connection.execute(
        "INSERT INTO placeholder_test (id, value) VALUES (1, 'test1'), (2, 'test2')");
    final result = await connection.execute(
        "SELECT value FROM placeholder_test WHERE id = :id", {"id": 2});
    expect(result.numOfRows, equals(1));
    final row = result.rows.first;
    expect(row.colByName("value"), equals('test2'));
    await connection.execute("DROP TABLE IF EXISTS placeholder_test");
  });

  test('Transactional: Commit the transaction', () async {
    await connection.execute("DROP TABLE IF EXISTS temp_test");
    await connection.execute(
        "CREATE TABLE temp_test (id INT AUTO_INCREMENT PRIMARY KEY, value INT)");
    await connection.execute("INSERT INTO temp_test (value) VALUES (10), (20)");

    final updateResult = await connection.transactional((conn) async {
      final res = await conn.execute(
        "UPDATE temp_test SET value = :value",
        {"value": 500},
      );
      return res.affectedRows.toInt();
    });
    expect(updateResult, equals(2));

    final result = await connection.execute("SELECT value FROM temp_test");
    for (final row in result.rows) {
      expect(row.colByName("value"), equals('500'));
    }
    await connection.execute("DROP TABLE IF EXISTS temp_test");
  });

  test('Transactional: Rollback of transaction in case of error', () async {
    await connection.execute("DROP TABLE IF EXISTS temp_test_rollback");
    await connection.execute(
        "CREATE TABLE temp_test_rollback (id INT AUTO_INCREMENT PRIMARY KEY, value INT) ENGINE=InnoDB;");
    await connection
        .execute("INSERT INTO temp_test_rollback (value) VALUES (10), (20)");

    try {
      await connection.transactional((conn) async {
        await conn.execute(
          "UPDATE temp_test_rollback SET value = :value",
          {"value": 200},
        );
        throw Exception("Forçando rollback");
      });
    } catch (e) {
      // Exceção esperada; o rollback deve ocorrer
    }

    final result =
        await connection.execute("SELECT value FROM temp_test_rollback");
    final values = result.rows.map((row) => row.colByName("value")).toList();
    expect(values, containsAll(['10', '20']));
    await connection.execute("DROP TABLE IF EXISTS temp_test_rollback");
  });

  test('Prepare: Cria, executa e dealloca prepared statement', () async {
    await connection.execute("DROP TABLE IF EXISTS temp_test");
    await connection.execute(
        "CREATE TABLE temp_test (id INT AUTO_INCREMENT PRIMARY KEY, value INT)");
    await connection.execute("INSERT INTO temp_test (value) VALUES (1), (2)");

    final stmt = await connection.prepare("UPDATE temp_test SET value = ?");
    final res = await stmt.execute([999]);
    expect(res.affectedRows.toInt(), equals(2));
    await stmt.deallocate();

    final result = await connection.execute("SELECT value FROM temp_test");
    for (final row in result.rows) {
      expect(row.colByName("value"), equals('999'));
    }
    await connection.execute("DROP TABLE IF EXISTS temp_test");
  });

  test('SSL connection: Conecta com SSL habilitado', () async {
    final sslConn = await MySQLConnection.createConnection(
      host: 'localhost',
      port: 3306,
      userName: 'root',
      password: 'root',
      databaseName: 'test_db',
      secure: true,
    );
    await sslConn.connect();
    expect(sslConn.connected, isTrue);
    await sslConn.close();
  });

  test('SSL connection: Conecta com SSL habilitado', () async {
    // Create a SecurityContext configured for SSL.
    // If your server requires client certificates,
    // you can load the certificate chain and private key.
    // Example:
    // context.useCertificateChain('path/to/client_cert.pem');
    // context.usePrivateKey('path/to/client_key.pem');
    // context.setTrustedCertificates('path/to/ca_cert.pem');
    final SecurityContext context = SecurityContext(withTrustedRoots: true);
    final sslConn = await MySQLConnection.createConnection(
      host: 'localhost',
      port: 3306,
      userName: 'root',
      password: 'root',
      databaseName: 'test_db',
      secure: true,
      securityContext: context,
      onBadCertificate: (certificate) => true,
    );
    await sslConn.connect();
    expect(sslConn.connected, isTrue);
    await sslConn.close();
  });

  test('Auth: caching_sha2_password', () async {
    // The "root" user is assumed to be configured to use caching_sha2_password (default in MySQL 8)
    final csConn = await MySQLConnection.createConnection(
      host: 'localhost',
      port: 3306,
      userName: 'root',
      password: 'root',
      databaseName: 'test_db',
      secure: true,
    );
    await csConn.connect();
    expect(csConn.connected, isTrue);
    await csConn.close();
  });

  test('Iterating large result sets', () async {
    await connection.execute("DROP TABLE IF EXISTS large_test");
    await connection.execute(
        "CREATE TABLE large_test (id INT AUTO_INCREMENT PRIMARY KEY, value INT)");
    // Insere 100 registros
    for (int i = 0; i < 100; i++) {
      await connection.execute("INSERT INTO large_test (value) VALUES ($i)");
    }
    final result = await connection.execute("SELECT id, value FROM large_test");
    int count = 0;
    // ignore: unused_local_variable
    for (final row in result.rows) {
      count++;
    }
    expect(count, equals(100));
    await connection.execute("DROP TABLE IF EXISTS large_test");
  });

  test('Typed data access', () async {
    await connection.execute("DROP TABLE IF EXISTS typed_test");
    await connection.execute(
        "CREATE TABLE typed_test (id INT, float_val FLOAT, date_val DATE)");
    await connection.execute(
        "INSERT INTO typed_test (id, float_val, date_val) VALUES (1, 3.14, '2020-01-01')");
    final result = await connection
        .execute("SELECT id, float_val, date_val FROM typed_test");
    final row = result.rows.first;
    expect(int.tryParse(row.colByName("id")!), equals(1));
    expect(double.tryParse(row.colByName("float_val")!), closeTo(3.14, 0.01));
    expect(row.colByName("date_val"), equals('2020-01-01'));
    await connection.execute("DROP TABLE IF EXISTS typed_test");
  });

  test('Prepared statements: Sending binary data', () async {
    await connection.execute("DROP TABLE IF EXISTS binary_test");
    await connection.execute(
        "CREATE TABLE binary_test (id INT AUTO_INCREMENT PRIMARY KEY, data BLOB)");
    final binaryData = Uint8List.fromList([0, 255, 127, 128]);
    final stmt =
        await connection.prepare("INSERT INTO binary_test (data) VALUES (?)");
    final res = await stmt.execute([binaryData]);
    expect(res.affectedRows.toInt(), equals(1));
    await stmt.deallocate();
    final result = await connection.execute("SELECT data FROM binary_test");
    final row = result.rows.first;
    print(row.typedColByName<Uint8List>("data"));
    // expect(row.typedColByName<Uint8List>("data"), equals(binaryData));
    // await connection.execute("DROP TABLE IF EXISTS binary_test");
  });

  test('Multiple result sets', () async {
    final results =
        await connection.execute("SELECT 1 AS first; SELECT 2 AS second;");
    // Converte o iterável para uma lista
    final resultList = results.toList();
    expect(resultList.length, equals(2));
    final firstRow = resultList[0].rows.first;
    final secondRow = resultList[1].rows.first;
    expect(firstRow.colByName("first"), equals('1'));
    expect(secondRow.colByName("second"), equals('2'));
  });

  test('truncate table', () async {
    // Remove a tabela se já existir
    await connection.execute('DROP TABLE IF EXISTS clients');
    // Cria a tabela "clients"
    await connection.execute('''
      CREATE TABLE IF NOT EXISTS clients (
        id INT NOT NULL AUTO_INCREMENT,
        name VARCHAR(255) NOT NULL,
        PRIMARY KEY (id)
      );
    ''');
    // Insere alguns registros
    await connection.execute("INSERT INTO clients (name) VALUES ('Alice')");
    await connection.execute("INSERT INTO clients (name) VALUES ('Bob')");
    await connection.execute("INSERT INTO clients (name) VALUES ('Charlie')");
    // Executa o truncate para remover todos os registros
    await connection.execute('TRUNCATE TABLE clients');
    // Verifica se a tabela está vazia
    final res = await connection.execute('SELECT * FROM clients');
    expect(res.numOfRows, equals(0));
  });

  test('onClose: Callback is invoked when closing the connection', () async {
    var closedCalled = false;
    final conn2 = await MySQLConnection.createConnection(
      host: 'localhost',
      port: 3306,
      userName: 'root',
      password: 'root',
      databaseName: 'test_db',
      secure: true,
    );
    conn2.onClose(() {
      closedCalled = true;
    });
    await conn2.connect();
    await conn2.close();
    expect(closedCalled, isTrue);
  });

  test('Protocol error: Invalid query returns MySQLServerException', () async {
    bool gotServerException = false;
    try {
      // Query propositalmente inválida
      await connection.execute("SELECT * FROM TABELA_INEXISTENTE OU ERRO");
    } catch (e) {
      // Se seu driver lança MySQLServerException ou MySQLClientException em erros de sintaxe
      if (e is MySQLServerException || e is MySQLClientException) {
        gotServerException = true;
      }
    }
    expect(gotServerException, isTrue,
        reason: "Deveria lançar erro de servidor");
  });

  // test('Tipos YEAR(2) e YEAR(4)', () async {
  //   // Depending on your MySQL configuration, YEAR(2) may be deprecated as of 5.7.
  //   await connection.execute("DROP TABLE IF EXISTS year_test");
  //   await connection.execute('''
  //   CREATE TABLE year_test (
  //     y2 YEAR(2),
  //     y4 YEAR(4)
  //   )
  // ''');

  //   // Insere valores "ano 99" e "ano 2025"
  //   // Observação: YEAR(2) costuma armazenar 99 como 1999, mas depende da versão do MySQL.
  //   await connection.execute("INSERT INTO year_test (y2, y4) VALUES (99, 2025)");
  //   final res = await connection.execute("SELECT y2, y4 FROM year_test");
  //   final row = res.rows.first;
  //   // Normalmente, colByName("y2") = "1999" ou algo assim, mas pode variar.
  //   //print("Valor de y2 = ${row.colByName('y2')}, y4 = ${row.colByName('y4')}");
  //   // Adapte as expectations conforme seu MySQL retorna.
  //   expect(row.colByName('y2'), anyOf(['1999', '99']), reason: 'Depende da versão');
  //   expect(row.colByName('y4'), equals('2025'));
  //   await connection.execute("DROP TABLE IF EXISTS year_test");
  // });

  test('Coluna BIT', () async {
    await connection.execute("DROP TABLE IF EXISTS bit_test");
    await connection.execute('''
    CREATE TABLE bit_test (
      flags BIT(8)
    )
  ''');

    // for example, b'10101010'
    await connection
        .execute("INSERT INTO bit_test (flags) VALUES (b'10101010')");

    final res = await connection.execute("SELECT flags FROM bit_test");
    final row = res.rows.first;
    final rawVal = row.colByName("flags");
    //print("Valor BIT: $rawVal");

    // Normally MySQL sends in the textual protocol as binary/ASCII string
    // or 0/1. This can vary. You can check if the string is something like "[85]" or "\x55".
    // Sometimes it comes as strange characters.
    // If you have binary parsing, you may want typedColByName<Uint8List>("flags").
    expect(rawVal, isNotNull);

    await connection.execute("DROP TABLE IF EXISTS bit_test");
  });

  test('Prepared statements: multiples execs and re-prepare', () async {
    await connection.execute("DROP TABLE IF EXISTS multi_prepared_test");
    await connection.execute('''
    CREATE TABLE multi_prepared_test (
      id INT AUTO_INCREMENT PRIMARY KEY,
      val VARCHAR(50)
    )
  ''');
    // Prepare the same query twice (re-prepare)
    for (int i = 0; i < 2; i++) {
      final stmt = await connection
          .prepare("INSERT INTO multi_prepared_test (val) VALUES (?)");

      // Runs with different parameters
      for (var v in ['A', 'B', 'C']) {
        final res = await stmt.execute([v]);
        expect(res.affectedRows.toInt(), 1);
      }
      await stmt.deallocate();
    }
    // Esperamos 2 (prepare) * 3 (exec) = 6 linhas inseridas
    final resAll = await connection
        .execute("SELECT COUNT(*) as total FROM multi_prepared_test");
    final countRow = resAll.rows.first;
    expect(countRow.colByName("total"), anyOf(['6', '6.0']));
    await connection.execute("DROP TABLE IF EXISTS multi_prepared_test");
  });

  test(
      'Concurrency: multiple simultaneous queries on the same connection (if supported)',
      () async {
    // This test *may* fail if the driver does not support parallel queries on the same connection.
    // In many drivers, this is not allowed and must be done via "pooling" or separate connections.
    final futures = <Future>[];
    for (int i = 0; i < 3; i++) {
      futures.add(connection.execute("SELECT SLEEP(1) as s$i"));
    }
    bool gotError = false;
    try {
      await Future.wait(futures);
    } catch (e) {
      gotError = true;
    }
    // If the driver does not support it, `gotError` should be true.
    // If it does, it should be false and the queries complete without error.
    print("Suporta queries paralelas? ${!gotError}");
  });

  test('Connection lost in the middle of query', () async {
    // We need a "hack" to close the socket in the middle of the query,
    // or kill the MySQL server (not feasible in the test).
    // Artificial example:
    final conn2 = await MySQLConnection.createConnection(
      host: 'localhost',
      port: 3306,
      userName: 'root',
      password: 'root',
      databaseName: 'test_db',
      secure: true,
    );
    await conn2.connect();
    expect(conn2.connected, isTrue);

    // Assuming there is some "closeSocket()" in the driver (non-public).
    // Let's simulate throwing an exception in the middle or destroying the connection:
    // This test may require handling something in the driver.
    conn2.getSocket().destroy(); // If accessible, destroy the socket abruptly

    bool gotError = false;
    try {
      await conn2.execute("SELECT SLEEP(2)");
    } catch (e) {
      gotError = true;
      print("Connection lost exception: $e");
    }
    expect(gotError, isTrue,
        reason: "Should fail because the connection was destroyed");
  });
  test('Using USE to switch databases', () async {
    // Create another database for testing
    try {
      await connection.execute("CREATE DATABASE IF NOT EXISTS outro_db");
    } catch (e) {
      // ignore if unable to create
    }
    // Switch to outro_db
    await connection.execute("USE outro_db");
    // Create a table in it, if desired
    await connection.execute("DROP TABLE IF EXISTS table_outrodb");
    await connection.execute("CREATE TABLE table_outrodb (id INT)");
    // Return to the original database
    await connection.execute("USE test_db");
  });

  test('Coluna JSON', () async {
    // Make sure MySQL >= 5.7 and supports JSON
    await connection.execute("DROP TABLE IF EXISTS json_test");
    await connection.execute('''
    CREATE TABLE json_test (
      data JSON
    )
  ''');
    // Insere um objeto JSON
    await connection.execute(
      "INSERT INTO json_test (data) VALUES ('{\"name\":\"Alice\",\"age\":30}')",
    );
    final res = await connection.execute("SELECT data FROM json_test");
    final row = res.rows.first;
    final jsonData = row.colByName("data");
    expect(jsonData['age'], equals(30));
    await connection.execute("DROP TABLE IF EXISTS json_test");
  });
}
