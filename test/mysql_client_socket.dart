import 'dart:io';
import 'mysql_client.dart';

void main() {
  // /var/run/mysqld/mysqld.sock
  testMysqlClient(
    InternetAddress('/tmp/mysql.sock', type: InternetAddressType.unix),
    3306,
    'root',
    'root',
    'test_db',
  );
}
