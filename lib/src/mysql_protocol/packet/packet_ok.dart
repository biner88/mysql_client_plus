import 'dart:typed_data';
import 'package:mysql_client_plus/mysql_protocol.dart';
import 'package:mysql_client_plus/mysql_protocol_extension.dart';

class MySQLPacketOK extends MySQLPacketPayload {
  final int header;
  final BigInt affectedRows;
  final BigInt lastInsertID;

  MySQLPacketOK({
    required this.header,
    required this.affectedRows,
    required this.lastInsertID,
  });

  factory MySQLPacketOK.decode(Uint8List buffer) {
    final byteData = ByteData.sublistView(buffer);
    int offset = 0;

    final header = byteData.getUint8(offset);
    offset += 1;

    final affectedRows = byteData.getVariableEncInt(offset);
    offset += affectedRows.item2;

    final lastInsertID = byteData.getVariableEncInt(offset);
    offset += lastInsertID.item2;

    return MySQLPacketOK(
      header: header,
      affectedRows: affectedRows.item1,
      lastInsertID: lastInsertID.item1,
    );
  }

  @override
  Uint8List encode() {
    throw UnimplementedError("Encode not implementado for MySQLPacketOK");
  }
}
