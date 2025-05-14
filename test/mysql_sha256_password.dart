import 'package:mysql_client_plus/mysql_client_plus.dart';
import 'package:test/test.dart';

void main() {
  MySQLConnection? conn;
  setUpAll(() async {
    // Create a connection with the bank
    conn = await MySQLConnection.createConnection(
      host: 'localhost',
      port: 3306,
      userName: 'your_user_sha256',
      password: 'your_password_sha256',
      databaseName: 'testdb',
      secure: true,
    );
    await conn!.connect();
  });
  tearDownAll(() async {
    try {
      await conn!.close();
    } catch (e) {
      // Log or ignore if the connection is not dated
    }
  });
  test('test sha256_password plugin', () async {
    await conn!.execute("DROP TABLE IF EXISTS sha256_password_test");
    await conn!.execute("CREATE TABLE sha256_password_test (id INT AUTO_INCREMENT PRIMARY KEY, value INT)");
    await conn!.execute("DROP TABLE IF EXISTS sha256_password_test");
  });
}
