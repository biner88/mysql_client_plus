import 'package:mysql_client_plus/mysql_client_plus.dart';

Future<void> main(List<String> arguments) async {
  print("Connecting to mysql server...");

  // create connection
  final conn = await MySQLConnection.createConnection(
    host: "localhost",
    port: 3306,
    userName: "your_user",
    password: "your_password",
    databaseName: "testdb",
  );
  await conn.connect();
//   await conn.execute('''
// CREATE DEFINER=`your_user`@`localhost` PROCEDURE `proc01`()
// begin
//     declare test_name varchar(20) default '';
//     set test_name = 'test data';
//     select test_name;
// end
//   ''');
  final res = await conn.execute("call proc01()");
  print(await (res.rows.first.assoc())['test_name']);
  await conn.close();
}
