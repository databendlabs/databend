SELECT 1 + 1 as a, 1 - 1, 1 * 1, 1 / 2;
SELECT -1, +1;
SELECT toTypeName(-toUInt32(1)), toTypeName(-toUInt64(1)), toTypeName(-toInt32(1)), toTypeName(-toFloat32(1)), toTypeName(+toUInt8(1));
