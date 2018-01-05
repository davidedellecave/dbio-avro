package ddc.dbio;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TreeMap;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import ddc.support.jdbc.schema.LiteDbColumn;
import ddc.support.jdbc.schema.LiteDbTable;
import ddc.support.util.StringInputStream;

public class SqlAvroTypeConversion {
//	private final static LogListener logger = new LogConsole(SqlAvroTypeConversion.class);
	private static final TreeMap<JDBCType, AvroManagedType> jdbcManagedMap = new TreeMap<>();
	private static final TreeMap<AvroManagedType, Schema.Type> managedAvroMap = new TreeMap<>();

	public static enum AvroManagedType {
		String, Bytes, Integer, Long, Float, Double, Boolean, Timestamp, Decimal, NULL
	};

	public Schema buildAvroSchema(LiteDbTable meta) throws AvroConversionException, IOException {
		ObjectMapper factory = new ObjectMapper();
		ObjectNode node = factory.createObjectNode();
		// put header
		node.put("namespace", getFirstTableName(meta.getTableName()));
		node.put("name", getLastTableName(meta.getTableName()));
		node.put("type", "record");
		ArrayNode fields = factory.createArrayNode();
		node.set("fields", fields);
		// put fields
		for (LiteDbColumn c : meta.getColumns()) {
			ObjectNode field = factory.createObjectNode();
			field.put("name", getLastTableName(c.getName()));
			Schema.Type avroType = managedAvroMap.get(jdbcManagedMap.get(c.getType()));
			if (avroType.equals(Schema.Type.NULL)) {
				throw new AvroConversionException("buildAvroSchema - type mismatch col:[" + c.getName() + "] + jdbc:[" + c.getType() + "]");
			}
			if (c.isNullable()) {
				ArrayNode types = factory.createArrayNode();
				types.add(avroType.toString().toLowerCase());
				types.add(Schema.Type.NULL.toString().toLowerCase());
				field.set("type", types);
			} else {
				field.put("type", avroType.toString().toLowerCase());
			}
			fields.add(field);
		}
		Schema avroSchema = new Schema.Parser().parse(new StringInputStream(node.toString()));
		return avroSchema;
	}

	private String getLastTableName(String source) {
		if (source.contains(".")) {
			return StringUtils.substringAfterLast(source, ".");
		}
		return source;
	}

	private String getFirstTableName(String source) {
		String s = source;
		if (s.contains(".")) {
			s = StringUtils.substringBeforeLast(s, ".");
			s = StringUtils.replace(s, ".", "_");
			;
		}
		return s;
	}

	public void setAvroField(LiteDbTable sourceTable, GenericRecord record, String colName, Object value) throws AvroConversionException {

		if (value == null) {
			record.put(colName, null);
			return;
		}

		JDBCType jdbcType = sourceTable.getColumn(colName).getType();
		AvroManagedType mType = jdbcManagedMap.get(jdbcType);
		if (mType.equals(AvroManagedType.NULL)) {
			throw new AvroConversionException("setAvroField - type mismatch col:[" + colName + "] + jdbc:[" + jdbcType + "] managedType:[" + mType + "]");
		}
		Schema.Type avroType = getAvroType(record.getSchema(), colName);
		//
		if (!managedAvroMap.get(mType).equals(avroType)) {
			throw new AvroConversionException("setAvroField - type mismatch col:[" + colName + "] + managedType:[" + mType + "] avroType:[" + avroType + "]");
		}
		// logger.debug(colName + " " + jdbcType + " " + value);

		switch (mType) {
		case Boolean:
			if (value instanceof Boolean) {
				record.put(colName, value);
			} else {
				record.put(colName, Boolean.valueOf(String.valueOf(value)));
			}
			break;
		case Bytes:
			record.put(colName, value);
			break;
		case Double:
			if (value instanceof Double) {
				record.put(colName, value);
			} else {
				record.put(colName, Double.valueOf(String.valueOf(value)));
			}
			break;
		case Float:
			if (value instanceof Float) {
				record.put(colName, value);
			} else {
				record.put(colName, Float.valueOf(String.valueOf(value)));
			}
			break;
		case Integer:
			if (value instanceof Integer) {
				record.put(colName, value);
			} else {
				record.put(colName, Integer.valueOf(String.valueOf(value)));
			}
			break;
		case Long:
			if (value instanceof Long) {
				record.put(colName, value);
			} else {
				record.put(colName, Long.valueOf(String.valueOf(value)));
			}
			break;
		case String:
			if (value instanceof String) {
				record.put(colName, value);
			} else {
				record.put(colName, String.valueOf(value));
			}
			break;
		case Timestamp:
			if (value instanceof Date) {
				Date d = (Date) value;
				record.put(colName, d.getTime());
			} else {
				throw new AvroConversionException("setAvroField - type mismatch col:[" + colName + "] + jdbc:[" + jdbcType + "] avroType:[" + avroType + "]");
			}
			break;
		case Decimal:
			record.put(colName, String.valueOf(value));
			break;
		case NULL:
			throw new AvroConversionException("setAvroField - type mismatch col:[" + colName + "] + jdbc:[" + jdbcType + "] managedType:[" + mType + "]");
		}
	}

	public List<Triple<JDBCType, Schema.Type, AvroManagedType>> buildTypes(Schema avroSchema, LiteDbTable targetTable) throws AvroConversionException {
		List<Triple<JDBCType, Schema.Type, AvroManagedType>> triples = new ArrayList<>(targetTable.getColumns().size());
		for (LiteDbColumn col : targetTable.getColumns()) {
			String colName = col.getName();
			JDBCType jdbcType = targetTable.getColumn(colName).getType();
			// int jdbcIndex = targetTable.getColumn(colName).getIndex();
			AvroManagedType mType = jdbcManagedMap.get(jdbcType);
			Schema.Type avroType = getAvroType(avroSchema, colName);
			if (!avroType.equals(managedAvroMap.get(mType))) {
				throw new AvroConversionException("setSqlField - type mismatch col:[" + colName + "] + avroType:[" + avroType + "] managedType:[" + mType + "]");
			}
			triples.add(ImmutableTriple.of(jdbcType, avroType, mType));
		}
		return triples;
	}

	public void setSqlField(Triple<JDBCType, Schema.Type, AvroManagedType> triple, int jdbcIndex, PreparedStatement trgStatement, Object value) throws SQLException, AvroConversionException {
		if (value == null) {
			trgStatement.setObject(jdbcIndex, null);
			return;
		}
		JDBCType jdbcType = triple.getLeft();
		Schema.Type avroType = triple.getMiddle();
		AvroManagedType mType = triple.getRight();
		switch (mType) {
		case Boolean:
			trgStatement.setBoolean(jdbcIndex, (Boolean) value);
			break;
		case Bytes:
			trgStatement.setBytes(jdbcIndex, (byte[]) value);
			break;
		case Double:
			trgStatement.setDouble(jdbcIndex, (Double) value);
			break;
		case Float:
			trgStatement.setFloat(jdbcIndex, (Float) value);
			break;
		case Integer:
			trgStatement.setInt(jdbcIndex, (Integer) value);
			break;
		case Long:
			trgStatement.setLong(jdbcIndex, (Long) value);
			break;
		case String:
			trgStatement.setString(jdbcIndex, String.valueOf(value));
			break;
		case Timestamp:
			if (avroType.equals(Schema.Type.LONG)) {
				trgStatement.setTimestamp(jdbcIndex, new Timestamp((Long) value));
			} else {
				throw new AvroConversionException("setSqlField - type mismatch col:[" + jdbcIndex + "] + jdbc:[" + jdbcType + "] managedType:[" + mType + "] avroType:[" + avroType + "]");
			}
			break;
		case Decimal:
			if (avroType.equals(Schema.Type.STRING)) {
				trgStatement.setBigDecimal(jdbcIndex, new BigDecimal(String.valueOf(value)));
			} else {
				throw new AvroConversionException("setSqlField - type mismatch col:[" + jdbcIndex + "] + jdbc:[" + jdbcType + "] managedType:[" + mType + "] avroType:[" + avroType + "]");
			}
			break;
		case NULL:
			throw new AvroConversionException("setSqlField - type mismatch col:[" + jdbcIndex + "] + jdbc:[" + jdbcType + "] managedType:[" + mType + "] avroType:[" + avroType + "]");
		}
	}

	public void setSqlField_safe(Schema avroSchema, LiteDbTable targetTable, PreparedStatement trgStatement, String colName, Object value) throws SQLException, AvroConversionException {
		JDBCType jdbcType = targetTable.getColumn(colName).getType();
		int jdbcIndex = targetTable.getColumn(colName).getIndex();
		AvroManagedType mType = jdbcManagedMap.get(jdbcType);
		Schema.Type avroType = getAvroType(avroSchema, colName);
		if (!avroType.equals(managedAvroMap.get(mType))) {
			throw new AvroConversionException("setSqlField - type mismatch col:[" + colName + "] + avroType:[" + avroType + "] managedType:[" + mType + "]");
		}
		switch (mType) {
		case Boolean:
			if (value instanceof Boolean) {
				trgStatement.setBoolean(jdbcIndex, (Boolean) value);
			} else {
				trgStatement.setBoolean(jdbcIndex, Boolean.valueOf(String.valueOf(value)));
			}
			break;
		case Bytes:
			if (value instanceof byte[]) {
				trgStatement.setBytes(jdbcIndex, (byte[]) value);
			} else {
				trgStatement.setBytes(jdbcIndex, String.valueOf(value).getBytes());
			}
			break;
		case Double:
			if (value instanceof Double) {
				trgStatement.setDouble(jdbcIndex, (Double) value);
			} else {
				trgStatement.setDouble(jdbcIndex, Double.valueOf(String.valueOf(value)));
			}
			break;
		case Float:
			if (value instanceof Float) {
				trgStatement.setFloat(jdbcIndex, (Float) value);
			} else {
				trgStatement.setFloat(jdbcIndex, Float.valueOf(String.valueOf(value)));
			}
			break;
		case Integer:
			if (value instanceof Integer) {
				trgStatement.setInt(jdbcIndex, (Integer) value);
			} else {
				trgStatement.setInt(jdbcIndex, Integer.valueOf(String.valueOf(value)));
			}
			break;
		case Long:
			if (value instanceof Long) {
				trgStatement.setLong(jdbcIndex, (Long) value);
			} else {
				trgStatement.setLong(jdbcIndex, Long.valueOf(String.valueOf(value)));
			}
			break;
		case String:
			if (value instanceof String) {
				trgStatement.setString(jdbcIndex, (String) value);
			} else {
				trgStatement.setString(jdbcIndex, String.valueOf(value));
			}
			break;
		case Timestamp:
			if (avroType.equals(Schema.Type.LONG)) {
				if (value instanceof Long) {
					trgStatement.setTimestamp(jdbcIndex, new Timestamp((Long) value));
				} else {
					trgStatement.setTimestamp(jdbcIndex, new Timestamp(Long.valueOf(String.valueOf(value))));
				}
			} else {
				throw new AvroConversionException("setSqlField - type mismatch col:[" + colName + "] + jdbc:[" + jdbcType + "] managedType:[" + mType + "] avroType:[" + avroType + "]");
			}
			break;
		case Decimal:
			if (avroType.equals(Schema.Type.STRING)) {
				trgStatement.setBigDecimal(jdbcIndex, new BigDecimal(String.valueOf(value)));
			} else {
				throw new AvroConversionException("setSqlField - type mismatch col:[" + colName + "] + jdbc:[" + jdbcType + "] managedType:[" + mType + "] avroType:[" + avroType + "]");
			}
			break;
		case NULL:
			throw new AvroConversionException("setSqlField - type mismatch col:[" + colName + "] + jdbc:[" + jdbcType + "] managedType:[" + mType + "]");
		}

	}

	private Schema.Type getAvroType(Schema schema, String colName) throws AvroConversionException {
		Field avroField =schema.getField(colName); 
		if (avroField==null) {
			throw new AvroConversionException("Column not found:[" + colName + "] schema:[" + schema.toString() + "]");
		}
		Schema.Type avroType = schema.getField(colName).schema().getType();
		if (avroType.equals(Schema.Type.UNION)) {
			if (schema.getField(colName).schema().getTypes() != null) {
				for (Schema t : schema.getField(colName).schema().getTypes()) {
					if (t.getType() != Schema.Type.NULL) {
						avroType = t.getType();
						break;
					}
				}
			}
		}
		return avroType;
	}

	static {
		// Managed Type
		jdbcManagedMap.put(JDBCType.ARRAY, AvroManagedType.NULL);
		jdbcManagedMap.put(JDBCType.BIGINT, AvroManagedType.Decimal);
		jdbcManagedMap.put(JDBCType.BINARY, AvroManagedType.Bytes);
		jdbcManagedMap.put(JDBCType.BIT, AvroManagedType.Boolean);
		jdbcManagedMap.put(JDBCType.BLOB, AvroManagedType.Bytes);
		jdbcManagedMap.put(JDBCType.BOOLEAN, AvroManagedType.Boolean);
		jdbcManagedMap.put(JDBCType.CHAR, AvroManagedType.String);
		jdbcManagedMap.put(JDBCType.CLOB, AvroManagedType.Bytes);
		jdbcManagedMap.put(JDBCType.DATALINK, AvroManagedType.NULL);
		jdbcManagedMap.put(JDBCType.DATE, AvroManagedType.Timestamp);
		jdbcManagedMap.put(JDBCType.DECIMAL, AvroManagedType.Decimal);
		jdbcManagedMap.put(JDBCType.DISTINCT, AvroManagedType.NULL);
		jdbcManagedMap.put(JDBCType.DOUBLE, AvroManagedType.Double);
		jdbcManagedMap.put(JDBCType.FLOAT, AvroManagedType.Float);
		jdbcManagedMap.put(JDBCType.INTEGER, AvroManagedType.Integer);
		jdbcManagedMap.put(JDBCType.JAVA_OBJECT, AvroManagedType.Bytes);
		jdbcManagedMap.put(JDBCType.LONGNVARCHAR, AvroManagedType.String);
		jdbcManagedMap.put(JDBCType.LONGVARBINARY, AvroManagedType.Bytes);
		jdbcManagedMap.put(JDBCType.LONGVARCHAR, AvroManagedType.String);
		jdbcManagedMap.put(JDBCType.NCHAR, AvroManagedType.String);
		jdbcManagedMap.put(JDBCType.NCLOB, AvroManagedType.String);
		jdbcManagedMap.put(JDBCType.NULL, AvroManagedType.NULL);
		jdbcManagedMap.put(JDBCType.NUMERIC, AvroManagedType.Decimal);
		jdbcManagedMap.put(JDBCType.NVARCHAR, AvroManagedType.String);
		jdbcManagedMap.put(JDBCType.OTHER, AvroManagedType.NULL);
		jdbcManagedMap.put(JDBCType.REAL, AvroManagedType.Double);
		jdbcManagedMap.put(JDBCType.REF, AvroManagedType.NULL);
		jdbcManagedMap.put(JDBCType.REF_CURSOR, AvroManagedType.NULL);
		jdbcManagedMap.put(JDBCType.ROWID, AvroManagedType.NULL);
		jdbcManagedMap.put(JDBCType.SMALLINT, AvroManagedType.Integer);
		jdbcManagedMap.put(JDBCType.SQLXML, AvroManagedType.String);
		jdbcManagedMap.put(JDBCType.STRUCT, AvroManagedType.NULL);
		jdbcManagedMap.put(JDBCType.TIME, AvroManagedType.NULL);
		jdbcManagedMap.put(JDBCType.TIME_WITH_TIMEZONE, AvroManagedType.NULL);
		jdbcManagedMap.put(JDBCType.TIMESTAMP, AvroManagedType.Timestamp);
		jdbcManagedMap.put(JDBCType.TINYINT, AvroManagedType.Integer);
		jdbcManagedMap.put(JDBCType.VARBINARY, AvroManagedType.Bytes);
		jdbcManagedMap.put(JDBCType.VARCHAR, AvroManagedType.String);
		//
		managedAvroMap.put(AvroManagedType.Boolean, Schema.Type.BOOLEAN);
		managedAvroMap.put(AvroManagedType.Bytes, Schema.Type.BYTES);
		managedAvroMap.put(AvroManagedType.Double, Schema.Type.DOUBLE);
		managedAvroMap.put(AvroManagedType.Float, Schema.Type.FLOAT);
		managedAvroMap.put(AvroManagedType.Integer, Schema.Type.INT);
		managedAvroMap.put(AvroManagedType.Long, Schema.Type.LONG);
		managedAvroMap.put(AvroManagedType.NULL, Schema.Type.NULL);
		managedAvroMap.put(AvroManagedType.String, Schema.Type.STRING);
		managedAvroMap.put(AvroManagedType.Timestamp, Schema.Type.LONG);
		managedAvroMap.put(AvroManagedType.Decimal, Schema.Type.STRING);
	}

}
