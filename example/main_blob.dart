import 'package:mysql_client_plus/mysql_client_plus.dart';

Future<void> main(List<String> arguments) async {
  print("Connecting to mysql server...");

  // create connection
  final conn = await MySQLConnection.createConnection(
    host: 'localhost',
    port: 3306,
    userName: 'your_user',
    password: 'your_password',
    databaseName: 'testdb',
    secure: true,
  );
  await conn.connect();
  await conn.execute("DROP TABLE IF EXISTS bit_test");
  await conn.execute('''
    CREATE TABLE bit_test (
      flags BIT(8)
    )
  ''');

  // for example, b'10101010'
  await conn.execute("INSERT INTO bit_test (flags) VALUES (b'10101010')");
  await conn.close();
}
