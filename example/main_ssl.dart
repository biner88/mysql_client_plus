import 'dart:io';

import 'package:mysql_client_plus/mysql_client_plus.dart';

Future<void> main(List<String> arguments) async {
  print("Connecting to mysql server...");

  final SecurityContext context = SecurityContext(withTrustedRoots: true);
  // context.useCertificateChain('path/to/client_cert.pem');
  // context.usePrivateKey('path/to/client_key.pem');
  // context.setTrustedCertificates('path/to/ca_cert.pem');
  // create connection
  final conn = await MySQLConnection.createConnection(
    host: 'localhost',
    port: 3306,
    userName: 'your_user',
    password: 'your_password',
    databaseName: 'testdb',
    secure: true,
    securityContext: context,
    onBadCertificate: (certificate) => true,
  );
  await conn.connect();
  print("Connected");
  await conn.close();
}
