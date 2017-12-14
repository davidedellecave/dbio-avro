package ddc.dbimp;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

public class AvroReader {
	
	
	public static void main(String[] args) throws InterruptedException, IOException {
		AvroReader r = new AvroReader();
	
		File schemaFile = new File("/Users/davide/tmp/out/dataquality.RET_farm_anagrafica.avsc");
		File avroFile = new File("/Users/davide/tmp/out/dataquality.RET_farm_anagrafica.snappy.avro");
		
		Schema schema = new Schema.Parser().parse(schemaFile);
		
		r.readAvro(avroFile, schema);
		
	}
	
	public void readAvro(File avroFile, Schema schema) throws IOException {
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
		try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(avroFile, datumReader)) {
			GenericRecord record = null;
			while (dataFileReader.hasNext()) {
				record = dataFileReader.next(record);
				System.out.println(record);
			}
		}
	}
	
}
