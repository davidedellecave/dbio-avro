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
		String signature = getSignature(pool, tableConf, conf.getOverrideMaxRows());
		
		if (StringUtils.isNotBlank(conf.getSourceFileTemplate())) {
			VarEval ve = new VarEval();
			signature += "." + ve.eval(conf.getSourceFileTemplate());
		}
		
		if (conf.getAvroCodeFactory().equals(CodecFactory.nullCodec())) {
			signature += ".avro";
		} else {
			signature += "." + conf.getAvroCodeFactory().toString() + ".avro";
		}
		
		return Paths.get(conf.getSourceFolder() + "/" + signature);
	}
	
	public static Path getSource(DbImp_ConsoleConfig conf, TablePool2Config pool, TableConfig tableConf, String ext) {
		String signature = getSignature(pool, tableConf, conf.getOverrideMaxRows());
		if (StringUtils.isNotBlank(conf.getSourceFileTemplate())) {
			VarEval ve = new VarEval();
			signature += "." + ve.eval(conf.getSourceFileTemplate());
		}
		signature += "." + ext;
		return Paths.get(conf.getSourceFolder() + "/" + signature);
	}
	
	public static Path getAvroTarget(DbExp_ConsoleConfig conf, TablePool2Config pool, TableConfig tableConf) {
		String signature = getSignature(pool, tableConf, conf.getOverrideMaxRows());
		if (StringUtils.isNotBlank(conf.getTargetFileTemplate())) {
			VarEval ve = new VarEval();
			signature += "." + ve.eval(conf.getTargetFileTemplate());
		}
		if (conf.getAvroCodeFactory().equals(CodecFactory.nullCodec())) {
			signature += ".avro";
		} else {
			signature += "." + conf.getAvroCodeFactory().toString() + ".avro";
		}
		return Paths.get(conf.getTargetFolder() + "/" + signature);
	}

	public static Path getTarget(DbExp_ConsoleConfig conf, TablePool2Config pool, TableConfig tableConf, String ext) {
		String signature = getSignature(pool, tableConf, conf.getOverrideMaxRows());
		if (StringUtils.isNotBlank(conf.getTargetFileTemplate())) {
			VarEval ve = new VarEval();
			signature += "." + ve.eval(conf.getTargetFileTemplate());
		}
		signature += "." + ext;
		return Paths.get(conf.getTargetFolder() + "/" + signature);
	}

	public static String getSignature(TablePool2Config pool, TableConfig tableConf, int overrideMaxRows) {
		String sign = pool.getJdbcFactory().getDatabase() + "." + tableConf.getTable();
		if (overrideMaxRows > 0) {
			sign += "_" + overrideMaxRows;
		} else if (tableConf.getMaxrows() > 0) {
			sign += "_" + tableConf.getMaxrows();
		}
		return sign;
	}

}
