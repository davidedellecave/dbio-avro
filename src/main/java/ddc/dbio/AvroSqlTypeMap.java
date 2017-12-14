package ddc.dbio;

import java.sql.JDBCType;

import org.apache.avro.Schema;

import ddc.support.jdbc.db.SqlTypeMap;

public class AvroSqlTypeMap extends SqlTypeMap {
	private static final long serialVersionUID = -7066765127367971041L;

	public AvroSqlTypeMap() {
		put(JDBCType.ARRAY, Schema.Type.ARRAY.toString());
		put(JDBCType.BIGINT, Schema.Type.LONG.toString());
		put(JDBCType.BINARY, Schema.Type.BYTES.toString());
		put(JDBCType.BIT, Schema.Type.BOOLEAN.toString());
		put(JDBCType.BLOB, Schema.Type.BYTES.toString());
		put(JDBCType.BOOLEAN, Schema.Type.BOOLEAN.toString());
		put(JDBCType.CHAR, Schema.Type.STRING.toString());
		put(JDBCType.CLOB, Schema.Type.STRING.toString());
		put(JDBCType.DATALINK, "_DATALINK_");
		put(JDBCType.DATE, Schema.Type.STRING.toString());
		put(JDBCType.DECIMAL, Schema.Type.FLOAT.toString());
		put(JDBCType.DISTINCT, "_DISTINCT_");
		put(JDBCType.DOUBLE, Schema.Type.DOUBLE.toString());
		put(JDBCType.FLOAT, Schema.Type.FLOAT.toString());
		put(JDBCType.INTEGER, Schema.Type.INT.toString());
		put(JDBCType.JAVA_OBJECT, Schema.Type.BYTES.toString());
		put(JDBCType.LONGNVARCHAR, Schema.Type.STRING.toString());
		put(JDBCType.LONGVARBINARY, Schema.Type.BYTES.toString());
		put(JDBCType.NCHAR, Schema.Type.STRING.toString());
		put(JDBCType.NCLOB, Schema.Type.BYTES.toString());
		put(JDBCType.NULL, Schema.Type.NULL.toString());
		// Forcing cast
		put(JDBCType.NUMERIC, Schema.Type.STRING.toString());
		put(JDBCType.NVARCHAR, Schema.Type.STRING.toString());
		// Forcing cast
		put(JDBCType.OTHER, Schema.Type.STRING.toString());
		put(JDBCType.REAL, Schema.Type.FLOAT.toString());
		put(JDBCType.REF, "_REF_");
		put(JDBCType.REF_CURSOR, Schema.Type.NULL.toString());
		put(JDBCType.ROWID, Schema.Type.LONG.toString());
		put(JDBCType.SMALLINT, Schema.Type.INT.toString());
		put(JDBCType.SQLXML, Schema.Type.STRING.toString());
		put(JDBCType.STRUCT, Schema.Type.RECORD.toString());
		put(JDBCType.TIME, Schema.Type.LONG.toString());
		put(JDBCType.TIME_WITH_TIMEZONE, Schema.Type.LONG.toString());
		put(JDBCType.TIMESTAMP, Schema.Type.LONG.toString());
		put(JDBCType.TIMESTAMP_WITH_TIMEZONE, Schema.Type.LONG.toString());
		put(JDBCType.TINYINT, Schema.Type.INT.toString());
		put(JDBCType.VARBINARY, Schema.Type.STRING.toString());
		put(JDBCType.VARCHAR, Schema.Type.STRING.toString());
	}
}
