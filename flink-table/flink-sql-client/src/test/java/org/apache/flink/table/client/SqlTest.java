package org.apache.flink.table.client;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.ExecutorFactory;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.delegation.PlannerFactory;
import org.apache.flink.table.factories.ComponentFactoryService;
import org.apache.flink.table.module.ModuleManager;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

public class SqlTest {
	@Test
	public void test_hive_dialect() throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
		EnvironmentSettings settings = EnvironmentSettings.newInstance()
			.inBatchMode()
			.useBlinkPlanner()
			.build();
		TableConfig config = new TableConfig();
		config.setSqlDialect(SqlDialect.HIVE);
		// 无法设置将tableConfig设置进tableEnvironment
//		TableEnvironmentImpl tableEnvironment = TableEnvironmentImpl.create(settings);
		//reflect
//		TableEnvironmentImpl tableEnvironment = SqlTest.create(settings, config);

		//extends
		TableEnvironmentImpl tableEnvironment = MyTableEnvironment.create(settings, config);
		tableEnvironment.sqlUpdate("CREATE EXTERNAL TABLE IF NOT EXISTS test" +
			"(" +
			"id VARCHAR(128) COMMENT 'ID编号'," +
			"name VARCHAR(128) COMMENT '姓名'," +
			"age INT COMMENT '年龄'" +
			") COMMENT '测试表'" +
			"partitioned by (age int) " +
			"ROW FORMAT DELIMITED " +
			"FIELDS TERMINATED BY '\\n' " +
			"LINES TERMINATED BY ',' " +
			"STORED AS ORC " +
			"LOCATION '/data/hive_data1/' " +
			"TBLPROPERTIES('orc.compress'='ZLIB')");
	}

	public static TableEnvironmentImpl create(EnvironmentSettings settings, TableConfig tableConfig) throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {

		// temporary solution until FLINK-15635 is fixed
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

//		TableConfig tableConfig = new TableConfig();

		ModuleManager moduleManager = new ModuleManager();

		CatalogManager catalogManager = CatalogManager.newBuilder()
			.classLoader(classLoader)
			.config(tableConfig.getConfiguration())
			.defaultCatalog(
				settings.getBuiltInCatalogName(),
				new GenericInMemoryCatalog(
					settings.getBuiltInCatalogName(),
					settings.getBuiltInDatabaseName()))
			.build();

		FunctionCatalog functionCatalog = new FunctionCatalog(tableConfig, catalogManager, moduleManager);

		Map<String, String> executorProperties = settings.toExecutorProperties();
		Executor executor = ComponentFactoryService.find(ExecutorFactory.class, executorProperties)
			.create(executorProperties);

		Map<String, String> plannerProperties = settings.toPlannerProperties();
		Planner planner = ComponentFactoryService.find(PlannerFactory.class, plannerProperties)
			.create(
				plannerProperties,
				executor,
				tableConfig,
				functionCatalog,
				catalogManager);

//		return new TableEnvironmentImpl(
//			catalogManager,
//			moduleManager,
//			tableConfig,
//			executor,
//			functionCatalog,
//			planner,
//			settings.isStreamingMode(),
//			classLoader
//		);
		// if class is private
		Class<?> TableEnvironmentImpl = Class.forName("org.apache.flink.table.api.internal.TableEnvironmentImpl");
		Class<?> a = TableEnvironmentImpl.class;

		Constructor<org.apache.flink.table.api.internal.TableEnvironmentImpl> constructor =
			TableEnvironmentImpl.class.getDeclaredConstructor(CatalogManager.class,
			ModuleManager.class,
			TableConfig.class,
			Executor.class,
			FunctionCatalog.class,
			Planner.class,
			boolean.class,
			ClassLoader.class);
		constructor.setAccessible(true);
		return constructor.newInstance(catalogManager,
			moduleManager,
			tableConfig,
			executor,
			functionCatalog,
			planner,
			settings.isStreamingMode(),
			classLoader);
	}

	public static class MyTableEnvironment extends TableEnvironmentImpl {
		public MyTableEnvironment(CatalogManager catalogManager, ModuleManager moduleManager, TableConfig tableConfig, Executor executor, FunctionCatalog functionCatalog, Planner planner, boolean isStreamingMode, ClassLoader userClassLoader) {
			super(catalogManager, moduleManager, tableConfig, executor, functionCatalog, planner, isStreamingMode, userClassLoader);
		}

		public static TableEnvironmentImpl create(EnvironmentSettings settings, TableConfig tableConfig) {
			// temporary solution until FLINK-15635 is fixed
			ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

//			TableConfig tableConfig = new TableConfig();

			ModuleManager moduleManager = new ModuleManager();

			CatalogManager catalogManager = CatalogManager.newBuilder()
				.classLoader(classLoader)
				.config(tableConfig.getConfiguration())
				.defaultCatalog(
					settings.getBuiltInCatalogName(),
					new GenericInMemoryCatalog(
						settings.getBuiltInCatalogName(),
						settings.getBuiltInDatabaseName()))
				.build();

			FunctionCatalog functionCatalog = new FunctionCatalog(tableConfig, catalogManager, moduleManager);

			Map<String, String> executorProperties = settings.toExecutorProperties();
			Executor executor = ComponentFactoryService.find(ExecutorFactory.class, executorProperties)
				.create(executorProperties);

			Map<String, String> plannerProperties = settings.toPlannerProperties();
			Planner planner = ComponentFactoryService.find(PlannerFactory.class, plannerProperties)
				.create(
					plannerProperties,
					executor,
					tableConfig,
					functionCatalog,
					catalogManager);

			return new MyTableEnvironment(
				catalogManager,
				moduleManager,
				tableConfig,
				executor,
				functionCatalog,
				planner,
				settings.isStreamingMode(),
				classLoader
			);

		}
	}
}
