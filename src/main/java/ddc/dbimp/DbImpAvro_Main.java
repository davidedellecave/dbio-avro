package ddc.dbimp;

import ddc.config.ArgsValue;
import ddc.config.PlatformVars;
import ddc.dbio.DbIO_BuildSqlSchemaTask;
import ddc.task.exec.TaskExecutor;
import ddc.task.exec.TaskPool;
import ddc.task.exec.TaskPoolManager;
import ddc.task.exec.TaskSchema;
import ddc.task.impl.AppStatisticsTask;
import ddc.task.impl.ConfigurationTask;
import ddc.task.impl.FailTask;

public class DbImpAvro_Main extends TaskPool {
	private static final long serialVersionUID = -7483801095065534456L;

	public static void main(String[] args) throws InterruptedException {
		PlatformVars.NAME_PlatformPath="S2EP_HOME";
		DbImpAvro_Main main = new DbImpAvro_Main("db-import-avro", ExecutionType.Sequence);
		main.execute(args);
	}

	public DbImpAvro_Main(String id, ExecutionType poolType) {
		super(id, poolType);
	}

	private void execute(String[] args) throws InterruptedException {
		ArgsValue argsValue = new ArgsValue(args);

		TaskSchema schema1 = createSchema1();
		argsValue.setConfClass(DbImp_ConsoleConfig.class);
		TaskExecutor e = new TaskExecutor(schema1, argsValue);
		this.add(e);

		TaskPoolManager m = new TaskPoolManager();
		m.runSinglePool(this);
	}

	private TaskSchema createSchema1() {
		TaskSchema schema = new TaskSchema(AppStatisticsTask.class, FailTask.class);
		schema.nextSuccess(ConfigurationTask.class, FailTask.class)
		.nextSuccess(DbImp_SetupTask.class, FailTask.class)
		.nextSuccess(DbIO_BuildSqlSchemaTask.class, FailTask.class)
		.nextSuccess(DbImp_SqlInsertTask.class, FailTask.class);
		//
		return schema;
	}
}
