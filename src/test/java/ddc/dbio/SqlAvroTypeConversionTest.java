package ddc.dbio;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import junit.framework.TestCase;

public class SqlAvroTypeConversionTest extends TestCase {

	public void testSetAvroField() {
		SqlAvroTypeConversion conv = new SqlAvroTypeConversion();
		
//		GenericRecord rec = new GenericData.Record(avroSchema);
//		
//		conv.setAvroField(rec, index, jbdcType, avroType, value);
	}

}
