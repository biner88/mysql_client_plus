import 'dart:typed_data';
import 'package:mysql_client_plus/mysql_protocol.dart';

class MySQLPacketResultSet extends MySQLPacketPayload {
  final BigInt columnCount;
  final List<MySQLColumnDefinitionPacket> columns;
  final List<MySQLResultSetRowPacket> rows;

  MySQLPacketResultSet({
    required this.columnCount,
    required this.columns,
    required this.rows,
  });

  @override
  Uint8List encode() {
    throw UnimplementedError(
        "Encode not implementado for MySQLPacketResultSet");
  }
}
