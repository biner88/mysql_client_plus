import 'package:mysql_client_plus/mysql_client_plus.dart';
import 'package:test/test.dart';

void main() {
  MySQLConnection? conn;

  setUpAll(() async {
    // Create a connection with the bank
    conn = await MySQLConnection.createConnection(
      host: 'localhost',
      port: 3306,
      userName: 'root',
      password: 'root',
      databaseName: 'test_db',
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
  test('The connection must be established', () async {
    expect(conn!.connected, isTrue);
  });
  test('The stored procedure test', () async {
    await conn!.execute("DROP procedure IF EXISTS proc01");
    await conn!.execute('''
CREATE DEFINER=`root`@`localhost` PROCEDURE `proc01`()
begin
    declare test_name varchar(20) default '';
    set test_name = 'test data';  
    select test_name;
end
  ''');
    final res = await conn!.execute("call proc01()");
    expect(res.rows.first.assoc()['test_name'], equals('test data'));
    await conn!.execute("DROP procedure IF EXISTS proc01");
  });
}
