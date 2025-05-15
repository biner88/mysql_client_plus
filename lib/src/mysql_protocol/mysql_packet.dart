import 'dart:typed_data';
import 'package:buffer/buffer.dart' show ByteDataWriter;
import 'package:crypto/crypto.dart' as crypto;
import 'package:mysql_client_plus/exception.dart';
import 'package:mysql_client_plus/mysql_protocol.dart';
import 'package:tuple/tuple.dart' show Tuple2;

//
// Constantes de flags de capabilities do protocolo MySQL
//
const mysqlCapFlagClientLongPassword = 0x00000001;
const mysqlCapFlagClientFoundRows = 0x00000002;
const mysqlCapFlagClientLongFlag = 0x00000004;
const mysqlCapFlagClientConnectWithDB = 0x00000008;
const mysqlCapFlagClientNoSchema = 0x00000010;
const mysqlCapFlagClientCompress = 0x00000020;
const mysqlCapFlagClientODBC = 0x00000040;
const mysqlCapFlagClientLocalFiles = 0x00000080;
const mysqlCapFlagClientIgnoreSpace = 0x00000100;
const mysqlCapFlagClientProtocol41 = 0x00000200;
const mysqlCapFlagClientInteractive = 0x00000400;
const mysqlCapFlagClientSsl = 0x00000800;
const mysqlCapFlagClientIgnoreSigPipe = 0x00001000;
const mysqlCapFlagClientTransactions = 0x00002000;
const mysqlCapFlagClientReserved = 0x00004000;
const mysqlCapFlagClientSecureConnection = 0x00008000;
const mysqlCapFlagClientMultiStatements = 0x00010000;
const mysqlCapFlagClientMultiResults = 0x00020000;
const mysqlCapFlagClientPsMultiResults = 0x00040000;
const mysqlCapFlagClientPluginAuth = 0x00080000;
const mysqlCapFlagClientPluginAuthLenEncClientData = 0x00200000;
const mysqlCapFlagClientDeprecateEOF = 0x01000000;

const mysqlServerFlagMoreResultsExists = 0x0008;

/// Enum that represents or generic type of MySQL packet.
enum MySQLGenericPacketType {
  /// Package OK (header 0x00).
  ok,

  /// Error package (header 0xff).
  error,

  /// EOF package (header 0xfe).
  eof,

  /// Any other type of package is not identified.
  other
}

/// Interface that defines a MySQL package payload.
///
/// Each payload must be able to be [encoded] into a [Uint8List] for delivery.
abstract class MySQLPacketPayload {
  Uint8List encode();
}

/// Represents a complete MySQL package, containing head (4 bytes) and payload.
///
/// The package head consists of:
/// - 3 bytes for payload size.
/// - 1 byte for sequenceID.
/// O [payload] contains the real content of the package.
class MySQLPacket {
  /// Sequence ID of the package, used to guarantee the order of two packages.
  int sequenceID;

  /// Payload size (excluding the 4 bytes of the head).
  int payloadLength;

  /// Package contents.
  MySQLPacketPayload payload;

  MySQLPacket({
    required this.sequenceID,
    required this.payload,
    required this.payloadLength,
  });

  /// Retorna o tamanho total do pacote (cabeçalho de 4 bytes + payload).
  ///
  /// Lê os 3 primeiros bytes do [buffer] para calcular [payloadLength]
  /// e soma 4 (bytes do cabeçalho).
  static int getPacketLength(Uint8List buffer) {
    var header = ByteData(4)
      ..setUint8(0, buffer[0])
      ..setUint8(1, buffer[1])
      ..setUint8(2, buffer[2])
      ..setUint8(3, 0);
    final payloadLength = header.getUint32(0, Endian.little);
    return payloadLength + 4;
  }

  /// Decode the package head, returning (payloadLength, sequenceID).
  static Tuple2<int, int> decodePacketHeader(Uint8List buffer) {
    final byteData = ByteData.sublistView(buffer);
    // The first 3 bytes are for payloadLength.
    var header = ByteData(4)
      ..setUint8(0, buffer[0])
      ..setUint8(1, buffer[1])
      ..setUint8(2, buffer[2])
      ..setUint8(3, 0);
    final payloadLength = header.getUint32(0, Endian.little);

    // The 4th byte is the sequenceNumber.
    final sequenceNumber = byteData.getUint8(3);
    return Tuple2(payloadLength, sequenceNumber);
  }

  /// Detects the generic type of the package based on the first byte of the payload.
  ///
  /// Observing or payload:
  /// - 0x00 -> OK (payloadLength >= 7),
  /// - 0xfe -> EOF (payloadLength < 9),
  /// - 0xff -> Error,
  /// - Otherwise -> other.
  static MySQLGenericPacketType detectPacketType(Uint8List buffer) {
    final byteData = ByteData.sublistView(buffer);
    final header = decodePacketHeader(buffer);
    final payloadLength = header.item1;
    final type = byteData.getUint8(4);
    if (type == 0x00 && payloadLength >= 7) {
      return MySQLGenericPacketType.ok;
    } else if (type == 0xfe && payloadLength < 9) {
      return MySQLGenericPacketType.eof;
    } else if (type == 0xff) {
      return MySQLGenericPacketType.error;
    } else {
      return MySQLGenericPacketType.other;
    }
  }

  /// Decodes an initial handshake packet [MySQLPacketInitialHandshake].
  factory MySQLPacket.decodeInitialHandshake(Uint8List buffer) {
    final header = decodePacketHeader(buffer);
    final offset = 4;
    final payload = MySQLPacketInitialHandshake.decode(
      Uint8List.sublistView(buffer, offset),
    );
    return MySQLPacket(
      sequenceID: header.item2,
      payloadLength: header.item1,
      payload: payload,
    );
  }

  /// Decodes an Auth Switch Request packet [MySQLPacketAuthSwitchRequest].
  factory MySQLPacket.decodeAuthSwitchRequestPacket(Uint8List buffer) {
    final byteData = ByteData.sublistView(buffer);
    final header = decodePacketHeader(buffer);
    final offset = 4;
    final type = byteData.getUint8(offset);

    if (type != 0xfe) {
      throw MySQLProtocolException("Cannot decode AuthSwitchResponse packet: type is not 0xfe");
    }

    final payload = MySQLPacketAuthSwitchRequest.decode(
      Uint8List.sublistView(buffer, offset),
    );
    return MySQLPacket(
      sequenceID: header.item2,
      payloadLength: header.item1,
      payload: payload,
    );
  }

  /// Decodes a generic packet, which could be OK, EOF, ERROR, etc.
  factory MySQLPacket.decodeGenericPacket(Uint8List buffer) {
    final header = decodePacketHeader(buffer);
    final offset = 4;
    final byteData = ByteData.sublistView(buffer);
    final payloadLength = header.item1;
    final type = byteData.getUint8(offset);

    late MySQLPacketPayload payload;
    if (type == 0x00 && payloadLength >= 7) {
      payload = MySQLPacketOK.decode(
        Uint8List.sublistView(buffer, offset),
      );
    } else if (type == 0xfe && payloadLength < 9) {
      payload = MySQLPacketEOF.decode(
        Uint8List.sublistView(buffer, offset),
      );
    } else if (type == 0xff) {
      payload = MySQLPacketError.decode(
        Uint8List.sublistView(buffer, offset),
      );
    } else if (type == 0x01) {
      // Extra Auth Data
      payload = MySQLPacketExtraAuthData.decode(
        Uint8List.sublistView(buffer, offset),
      );
    } else {
      throw MySQLProtocolException("Unsupported generic packet: $buffer");
    }

    return MySQLPacket(
      sequenceID: header.item2,
      payloadLength: payloadLength,
      payload: payload,
    );
  }

  /// Decode a packet containing the column contagem [MySQLPacketColumnCount].
  factory MySQLPacket.decodeColumnCountPacket(Uint8List buffer) {
    final header = decodePacketHeader(buffer);
    final offset = 4;
    final byteData = ByteData.sublistView(buffer);
    final type = byteData.getUint8(offset);
    late MySQLPacketPayload payload;

    if (type == 0x00) {
      payload = MySQLPacketOK.decode(
        Uint8List.sublistView(buffer, offset),
      );
    } else if (type == 0xff) {
      payload = MySQLPacketError.decode(
        Uint8List.sublistView(buffer, offset),
      );
    } else if (type == 0xfb) {
      throw MySQLProtocolException(
        "COM_QUERY_RESPONSE of type 0xfb is not implemented",
      );
    } else {
      payload = MySQLPacketColumnCount.decode(
        Uint8List.sublistView(buffer, offset),
      );
    }

    return MySQLPacket(
      sequenceID: header.item2,
      payloadLength: header.item1,
      payload: payload,
    );
  }

  /// Decode a column definition packet [MySQLColumnDefinitionPacket].
  factory MySQLPacket.decodeColumnDefPacket(Uint8List buffer) {
    final header = decodePacketHeader(buffer);
    final offset = 4;
    final payload = MySQLColumnDefinitionPacket.decode(
      Uint8List.sublistView(buffer, offset),
    );
    return MySQLPacket(
      sequenceID: header.item2,
      payloadLength: header.item1,
      payload: payload,
    );
  }

  /// Decode a ResultSet line into textual format [MySQLResultSetRowPacket].
  factory MySQLPacket.decodeResultSetRowPacket(
    Uint8List buffer,
    List<MySQLColumnDefinitionPacket> colDefs,
  ) {
    final header = decodePacketHeader(buffer);
    final offset = 4;
    final payload = MySQLResultSetRowPacket.decode(
      Uint8List.sublistView(buffer, offset),
      colDefs,
    );
    return MySQLPacket(
      sequenceID: header.item2,
      payloadLength: header.item1,
      payload: payload,
    );
  }

  /// Decode a ResultSet line into binary format [MySQLBinaryResultSetRowPacket].
  factory MySQLPacket.decodeBinaryResultSetRowPacket(
    Uint8List buffer,
    List<MySQLColumnDefinitionPacket> colDefs,
  ) {
    final header = decodePacketHeader(buffer);
    final offset = 4;
    final payload = MySQLBinaryResultSetRowPacket.decode(
      Uint8List.sublistView(buffer, offset),
      colDefs,
    );
    return MySQLPacket(
      sequenceID: header.item2,
      payloadLength: header.item1,
      payload: payload,
    );
  }

  /// Decode response to COM_STMT_PREPARE [MySQLPacketStmtPrepareOK] or error.
  factory MySQLPacket.decodeCommPrepareStmtResponsePacket(Uint8List buffer) {
    final header = decodePacketHeader(buffer);
    final offset = 4;
    final byteData = ByteData.sublistView(buffer);
    final type = byteData.getUint8(offset);

    late MySQLPacketPayload payload;
    if (type == 0x00) {
      payload = MySQLPacketStmtPrepareOK.decode(
        Uint8List.sublistView(buffer, offset),
      );
    } else if (type == 0xff) {
      payload = MySQLPacketError.decode(
        Uint8List.sublistView(buffer, offset),
      );
    } else {
      throw MySQLProtocolException(
        "Unexpected header type while decoding COM_STMT_PREPARE response: $header",
      );
    }

    return MySQLPacket(
      sequenceID: header.item2,
      payloadLength: header.item1,
      payload: payload,
    );
  }

  /// Returns the truth of the payload for a package OK.
  bool isOkPacket() => payload is MySQLPacketOK;

  /// Returns the truth of the payload for an error packet.
  bool isErrorPacket() => payload is MySQLPacketError;

  /// Returns the truth of the payload for an EOF package.
  bool isEOFPacket() {
    if (payload is MySQLPacketEOF) {
      return true;
    }
    // Some servers send OK with header 0xfe and payloadLength < 9 as EOF
    if (payload is MySQLPacketOK && payloadLength < 9 && (payload as MySQLPacketOK).header == 0xfe) {
      return true;
    }
    return false;
  }

  /// Encode the package (head + payload) in a [Uint8List] for sending to the server.
  Uint8List encode() {
    final payloadData = payload.encode();
    // Prepare 4 bytes for the head:
    // 3 bytes for length, 1 for sequenceID.
    final header = ByteData(4);
    header.setUint8(0, payloadData.lengthInBytes & 0xFF);
    header.setUint8(1, (payloadData.lengthInBytes >> 8) & 0xFF);
    header.setUint8(2, (payloadData.lengthInBytes >> 16) & 0xFF);
    header.setUint8(3, sequenceID);

    final writer = ByteDataWriter(endian: Endian.little);
    writer.write(header.buffer.asUint8List());
    writer.write(payloadData);
    return writer.toBytes();
  }
}

/// Calculate or SHA1 hash two data [data].
List<int> sha1(List<int> data) {
  return crypto.sha1.convert(data).bytes;
}

/// Calculate or SHA256 hash two dice [data].
List<int> sha256(List<int> data) {
  return crypto.sha256.convert(data).bytes;
}

/// Performs an XOR operation between two byte arrays [aList] and [bList].
///
/// If an array is smaller, the missing bytes are considered 0.
/// Returns um [Uint8List] as the result of XOR byte by byte.
Uint8List xor(List<int> aList, List<int> bList) {
  final a = Uint8List.fromList(aList);
  final b = Uint8List.fromList(bList);
  if (a.isEmpty || b.isEmpty) {
    throw ArgumentError("Uint8List arguments must not be empty");
  }
  final length = a.length > b.length ? a.length : b.length;
  final buffer = Uint8List(length);

  for (int i = 0; i < length; i++) {
    final aa = i < a.length ? a[i] : 0;
    final bb = i < b.length ? b[i] : 0;
    buffer[i] = aa ^ bb;
  }
  return buffer;
}
