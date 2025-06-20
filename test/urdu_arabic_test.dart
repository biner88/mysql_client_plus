import 'dart:typed_data';

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
    await conn!.execute('DROP TABLE IF EXISTS `urdu_table`');
    await conn!.execute('''
    CREATE TABLE urdu_table (
        id INT AUTO_INCREMENT PRIMARY KEY,
        varchar_data VARCHAR(255),
        text_data TEXT,
        json_data JSON,
        blob_data BLOB
      )
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
    final textData = 'السلام علیکم';
    final blobData = Uint8List.fromList([0x41, 0x42, 0x43]);
    final req1 = await conn!.execute(
        "INSERT INTO urdu_table (varchar_data,text_data,json_data,blob_data) VALUES ('$textData','text data','{\"name\":\"میرا نام\",\"chinese_name\":\"我的名字\"}',0x414243)");
    expect(req1.affectedRows, BigInt.one);
    final stmt = await conn!.prepare(
        "INSERT INTO urdu_table (varchar_data,text_data,json_data,blob_data) VALUES (?,?,?,?)");
    final req2 = await stmt.execute([
      textData,
      'text data',
      '{"name":"میرا نام","chinese_name":"我的名字"}',
      blobData
    ]);
    expect(req2.affectedRows, BigInt.one);
  });
  test('Execute: select data ', () async {
    final textData = 'السلام علیکم';
    var req1 = await conn!.execute("SELECT * from urdu_table");
    expect(req1.rows.first.typedAssoc()['varchar_data'], textData);
    print(req1.rows.first.typedAssoc());

    ///
    // final stmt = await conn!.prepare('SELECT * from urdu_table');
    // final req2 = await stmt.execute([]);
    // print(req2.rows.first.typedAssoc());
    // expect(req2.rows.first.typedAssoc()['varchar_data'], textData);
    // await stmt.deallocate();
  });
  test('Execute: drop table ', () async {
    await conn!.execute("DROP TABLE IF EXISTS `urdu_table`");
  });
}
