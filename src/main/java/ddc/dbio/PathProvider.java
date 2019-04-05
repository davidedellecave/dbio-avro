package ddc.dbio;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.avro.file.CodecFactory;
import org.apache.commons.lang3.StringUtils;

import ddc.dbexp.DbExp_ConsoleConfig;
import ddc.dbimp.DbImp_ConsoleConfig;
import ddc.task.model.TableConfig;
import ddc.task.model.TablePool2Config;
import ddc.task.model.VarEval;

public class PathProvider {
	
	public static Path getAvroSource(DbImp_ConsoleConfig conf, TablePool2Config pool, TableConfig tableConf) {
		String filename = getFilenameWithoutExt(conf, pool, tableConf, conf.getSourceFileTemplate());
		if (conf.getAvroCodeFactory().equals(CodecFactory.nullCodec())) {
			filename += ".avro";
		} else {
			filename += "." + conf.getAvroCodeFactory().toString() + ".avro";
		}		
		return Paths.get(conf.getSourceFolder() + "/" + filename);
	}
	
	public static Path getSource(DbImp_ConsoleConfig conf, TablePool2Config pool, TableConfig tableConf, String ext) {		
		String filename = getFilenameWithoutExt(conf, pool, tableConf, conf.getSourceFileTemplate());
		filename += "." + ext;
		return Paths.get(conf.getSourceFolder() + "/" + filename);
	}

	public static Path getAvroTarget(DbExp_ConsoleConfig conf, TablePool2Config pool, TableConfig tableConf) {
		String filename = getFilenameWithoutExt(conf, pool, tableConf, conf.getTargetFileTemplate());
		
		if (conf.getAvroCodeFactory().equals(CodecFactory.nullCodec())) {
			filename += ".avro";
		} else {
			filename += "." + conf.getAvroCodeFactory().toString() + ".avro";
		}		
		return Paths.get(conf.getTargetFolder() + "/" + filename);
	}

	public static Path getTarget(DbExp_ConsoleConfig conf, TablePool2Config pool, TableConfig tableConf, String ext) {
		String filename = getFilenameWithoutExt(conf, pool, tableConf, conf.getTargetFileTemplate());
		filename += "." + ext;
		return Paths.get(conf.getTargetFolder() + "/" + filename);
	}
	
	private static String getFilenameWithoutExt(DbIO_Config conf, TablePool2Config pool, TableConfig tableConf, String template) {
		String filename = "";
		if (StringUtils.isNotBlank(template)) {
			filename = evaluate(pool, tableConf, template);
		} else {
			filename = tableConf.getTable();
		}
		filename += appendMaxRow(conf, tableConf);
		return filename;
	}

	public static String getSignature(DbIO_Config conf, TablePool2Config pool, TableConfig tableConf) {
		String database = pool.getJdbcFactory().getDatabase();
		String schema = pool.getSchema();
		String table = tableConf.getTable();
		String sig = "";
		sig += StringUtils.isNotBlank(database) ? database + "." : "";
		sig += StringUtils.isNotBlank(schema) ? schema + "." : "";
		sig += StringUtils.isNotBlank(table) ? table + "." : "";
		sig += appendMaxRow(conf, tableConf);
		return sig;
	}
	
	private static String appendMaxRow(DbIO_Config conf, TableConfig tableConf) {
		int overrideMaxRows = conf.getOverrideMaxRows();
		int tableMaxRow = tableConf.getMaxrows();
		if (overrideMaxRows > 0) {
			return "_" + overrideMaxRows;
		} else if (tableMaxRow > 0) {
			return "_" + tableMaxRow;
		}
		return "";
	}
	
	private static String evaluate(TablePool2Config pool, TableConfig tableConf, String source) {
		VarEval ve = new VarEval();
		String schema = pool.getSchema();
		ve.addVar("${SCHEMA}", StringUtils.isNotBlank(schema) ? schema : "");
		//
		String database = pool.getJdbcFactory().getDatabase();
		ve.addVar("${DATABASE}", StringUtils.isNotBlank(database) ? database : "");
		//
		String table = tableConf.getTable();
		ve.addVar("${TABLE}", StringUtils.isNotBlank(table) ? table : "");
		return ve.eval(source);
	}
	
//	public static String getSignature(TablePool2Config pool, TableConfig tableConf, int overrideMaxRows) {
//		String sign = pool.getJdbcFactory().getDatabase() + "." + tableConf.getTable();
//		if (overrideMaxRows > 0) {
//			sign += "_" + overrideMaxRows;
//		} else if (tableConf.getMaxrows() > 0) {
//			sign += "_" + tableConf.getMaxrows();
//		}
//		return sign;
//	}

}
