import 'package:mysql_client_plus/mysql_client_plus.dart';
import 'package:test/test.dart';

void main() {
  MySQLConnection? conn;

  setUpAll(() async {
    // Create a connection with the database
    conn = await MySQLConnection.createConnection(
      host: 'localhost',
      port: 3306,
      userName: 'root',
      password: 'root',
      databaseName: 'test_db',
      secure: true,
    );
    await conn!.connect();

    // Create a table with different columns for various data types
    await conn!.execute('DROP TABLE IF EXISTS `test_data5`');
    await conn!.execute('DROP TABLE IF EXISTS `test_data5_index`');
    await conn!.execute('''
    CREATE TABLE test_data5 (
        id INT AUTO_INCREMENT PRIMARY KEY,
        col_key VARCHAR(255),
        name VARCHAR(255)
      )
    ''');
    await conn!.execute('''
      CREATE TABLE test_data5_index (
        id INT AUTO_INCREMENT PRIMARY KEY,
        col_key VARCHAR(255),
        art_name VARCHAR(255)
      );
    ''');
  });

  tearDownAll(() async {
    try {
      await conn!.close();
    } catch (e) {
      // Log or ignore if the connection is not established
    }
  });

  test('Execute: insert data ', () async {
    await conn!.execute("INSERT INTO test_data5 (col_key, name) VALUES ('test_123', 'John Doe')");
    // await conn!.execute("INSERT INTO test_data5_index (col_key, art_name) VALUES ('test_123', 'Breaking CCK')");
  });

  test('Execute: left join', () async {
    var req1 = await conn!.execute("SELECT  cc.*,data.* FROM test_data5 AS data LEFT JOIN test_data5_index AS cc ON cc.col_key = data.col_key ORDER BY data.id DESC");
    expect(req1.rows.first.typedAssoc()['col_key'], equals('test_123'));
  });

  test('Execute: drop table ', () async {
    await conn!.execute("DROP TABLE IF EXISTS `test_data5`");
    await conn!.execute("DROP TABLE IF EXISTS `test_data5_index`");
  });
}
