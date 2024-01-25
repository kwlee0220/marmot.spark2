package marmot.spark.command;

import java.util.List;

import marmot.MarmotRuntime;
import marmot.dataset.DataSet;
import marmot.spark.MarmotSpark;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import utils.PicocliSubCommand;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SparkDatasetCommands {
	@Command(name="list", description="list datasets")
	public static class ListDataSet extends PicocliSubCommand<MarmotRuntime> {
		@Parameters(paramLabel="path", index="0", arity="0..1", description={"dataset folder path"})
		private String m_start;

		@Option(names={"-r"}, description="list all descendant datasets")
		private boolean m_recursive;

		@Option(names={"-l"}, description="list in detail")
		private boolean m_details;

		@Override
		public void run(MarmotRuntime initialContext) throws Exception {
			List<DataSet> dsList;
			if ( m_start != null ) {
				dsList = initialContext.getDataSetAllInDir(m_start, m_recursive);
				if ( dsList.isEmpty() ) {
					dsList.add(initialContext.getDataSet(m_start));
				}
			}
			else {
				dsList = initialContext.getDataSetAll();
			}
			
			for ( DataSet ds: dsList ) {
				System.out.print(ds.getId());
				
				if ( m_details ) {
					System.out.printf(" %s %s", ds.getType(), ds.getHdfsPath());
					if ( ds.hasGeometryColumn() ) {
						System.out.printf(" %s", ds.getGeometryColumnInfo());
						
						if ( ds.hasSpatialIndex() ) {
							System.out.printf("(clustered)");
						}
					}
				}
				System.out.println();
			}
		}
	}

//	@Command(name="show", description="print records of the dataset")
//	public static class Show extends SubCommand<MarmotRuntime> {
//		@Parameters(paramLabel="id", index="0", arity="1..1", description={"dataset id to print"})
//		private String m_dsId;
//
//		@Option(names={"-t", "-type"}, paramLabel="type",
//				description="target type: dataset (default), file, thumbnail")
//		private String m_type = "dataset";
//
//		@Option(names={"-project"}, paramLabel="column_list", description="selected columns (optional)")
//		private String m_cols = null;
//		
//		@Option(names={"-limit"}, paramLabel="count", description="limit count (optional)")
//		private int m_limit = -1;
//
//		@Option(names={"-csv"}, description="display csv format")
//		private boolean m_asCsv;
//
//		@Option(names={"-delim"}, paramLabel="character", description="csv delimiter (default: ',')")
//		private String m_delim = ",";
//
//		@Option(names={"-g", "-geom"}, description="display geometry columns")
//		private boolean m_displayGeom;
//		
//		@Override
//		public void run(MarmotRuntime initialContext) throws Exception {
//			PlanBuilder builder = Plan.builder("list_records");
//			switch ( m_type.toLowerCase() ) {
//				case "dataset":
//					builder = builder.load(m_dsId);
//					break;
//				case "thumbnail":
//					builder = builder.loadMarmotFile("database/thumbnails/" + m_dsId);
//					break;
//				case "file":
//					builder = builder.loadMarmotFile(m_dsId);
//					break;
//				default:
//					throw new IllegalArgumentException("invalid type: '" + m_type + "'");
//			}
//			
//			if ( m_limit > 0 ) {
//				builder = builder.take(m_limit);
//			}
//			if ( m_cols != null ) {
//				builder = builder.project(m_cols);
//			}
//		
//			if ( !m_displayGeom ) {
//				Plan tmp = builder.build();
//				RecordSchema schema = initialContext.getOutputRecordSchema(tmp);
//				String cols = schema.streamColumns()
//									.filter(col -> col.type().isGeometryType())
//									.map(Column::name)
//									.join(",");
//				if ( cols.length() > 0 ) {
//					builder.project(String.format("*-{%s}", cols));
//				}
//			}
//			
//			try ( RecordSet rset = initialContext.executeLocally(builder.build()) ) {
//				Record record = DefaultRecord.of(rset.getRecordSchema());
//				while ( rset.next(record) ) {
//					Map<String,Object> values = record.toMap();
//					
//					if ( m_asCsv ) {
//						System.out.println(toCsv(values.values(), m_delim));
//					}
//					else {
//						System.out.println(values);
//					}
//				}
//			}
//		}
//		
//		private static String toCsv(Collection<?> values, String delim) {
//			return values.stream()
//						.map(o -> {
//							String str = ""+o;
//							if ( str.contains(" ") || str.contains(delim) ) {
//								str = "\"" + str + "\"";
//							}
//							return str;
//						})
//						.collect(Collectors.joining(delim));
//		}
//	}

//	@Command(name="schema", description="print the RecordSchema of the dataset")
//	public static class Schema extends SubCommand<MarmotSpark> {
//		@Parameters(paramLabel="id", index="0", arity="1..1", description={"dataset id"})
//		private String m_dsId;
//
//		@Override
//		public void run(MarmotSpark marmot) throws Exception {
//			SparkDataSet ds = marmot.getDataSet(m_dsId);
//
//			System.out.println("TYPE         : " + ds.getType());
//			if ( ds.getRecordCount() > 0 ) {
//				System.out.println("COUNT        : " + ds.getRecordCount());
//			}
//			else {
//				System.out.println("COUNT        : unknown");
//			}
//			System.out.println("SIZE         : " + UnitUtils.toByteSizeString(ds.length()));
//			if ( ds.hasGeometryColumn() ) {
//				System.out.println("GEOMETRY     : " + ds.getGeometryColumnInfo().name());
//				System.out.println("SRID         : " + ds.getGeometryColumnInfo().srid());
//			}
//			System.out.println("HDFS PATH    : " + ds.getHdfsPath());
//			System.out.println("COMPRESSION  : " + ds.getCompressionCodecName().getOrElse("none"));
//
//			if ( ds.isSpatiallyClustered() ) {
//				System.out.println("SPATIAL CLUSTERS: " + ds.getClusterQuadKeyAll().size());
//			}
//			System.out.println("BLOCK_SIZE   : " + UnitUtils.toByteSizeString(ds.getBlockSize()));
//			
//			SpatialIndexInfo idxInfo = ds.getSpatialIndexInfo().getOrNull();
//			System.out.printf ("SPATIAL INDEX: %s%n", (idxInfo != null)
//														? idxInfo.getHdfsFilePath() : "none");
//			System.out.println("THUMBNAIL    : " + ds.hasThumbnail());
//			
//			System.out.println("COLUMNS      :");
//			ds.getRecordSchema().getColumns()
//					.stream()
//					.forEach(c -> System.out.println("\t" + c));
//		}
//	}

//	@Command(name="move", description="move a dataset to another directory")
//	public static class Move extends SubCommand<MarmotRuntime> {
//		@Parameters(paramLabel="id", index="0", arity="1..1", description={"id for the source dataset"})
//		private String m_src;
//		
//		@Parameters(paramLabel="path", description={"path to the destination path"})
//		private String m_dest;
//
//		@Override
//		public void run(MarmotRuntime initialContext) throws Exception {
//			DataSet srcDs = initialContext.getDataSet(m_src);
//			initialContext.moveDataSet(srcDs.getId(), m_dest);
//		}
//	}
//
//	@Command(name="set_geometry", description="set Geometry column info for a dataset")
//	public static class SetGcInfo extends SubCommand<MarmotRuntime> {
//		@Parameters(paramLabel="id", index="0", arity="1..1", description={"dataset id"})
//		private String m_dsId;
//		
//		@Parameters(paramLabel="col_name", index="1",
//					description={"name for default geometry column"})
//		private String m_column;
//		
//		@Parameters(paramLabel="EPSG_code", index="2",
//					description={"EPSG code for default geometry"})
//		private String m_srid;
//
//		@Override
//		public void run(MarmotRuntime initialContext) throws Exception {
//			DataSet ds = initialContext.getDataSet(m_dsId);
//			
//			GeometryColumnInfo gcInfo = new GeometryColumnInfo(m_column, m_srid);
//			ds.updateGeometryColumnInfo(FOption.ofNullable(gcInfo));
//		}
//	}

	@Command(name="count", description="count records of the dataset")
	public static class Count extends PicocliSubCommand<MarmotSpark> {
		@Parameters(paramLabel="id", index="0", arity="1..1", description={"dataset id"})
		private String m_dsId;
		
		@Option(names={"-v", "-verbose"}, description="verbose")
		private boolean m_verbose = false;

		@Override
		public void run(MarmotSpark marmot) throws Exception {
			StopWatch watch = StopWatch.start();
			
			long cnt = marmot.getDataSet(m_dsId)
							.read()
							.count();
			watch.stop();
			
			if ( m_verbose ) {
				System.out.printf("count=%d, elapsed=%s%n", cnt, watch.getElapsedMillisString());
			}
			else {
				System.out.println(cnt);
			}
		}
	}

//	@Command(name="bind", description="bind the existing file(s) as a dataset")
//	public static class Bind extends SubCommand<MarmotRuntime> {
//		@Parameters(paramLabel="path", index="0", arity="1..1",
//					description={"source file-path (or source dataset-id) to bind"})
//		private String m_path;
//		
//		@Parameters(paramLabel="dataset_id", index="1", arity="1..1",
//				description={"dataset id to bind into"})
//		private String m_dataset;
//		
//		@Option(names={"-t", "-type"}, paramLabel="type", required=true,
//				description={"source type ('text', 'file', or 'dataset)"})
//		private String m_type;
//
//		private GeometryColumnInfo m_gcInfo;
//		@Option(names={"-geom_col"}, paramLabel="column_name(EPSG code)",
//				description="default Geometry column info")
//		public void setGeometryColumnInfo(String gcInfoStr) {
//			m_gcInfo = GeometryColumnInfo.fromString(gcInfoStr);
//		}
//		
//		@Option(names={"-f", "-force"}, description="force to bind to a new dataset")
//		private boolean m_force;
//
//		@Override
//		public void run(MarmotRuntime initialContext) throws Exception {
//			DataSetType type;
//			switch ( m_type ) {
//				case "text":
//					type = DataSetType.TEXT;
//					break;
//				case "file":
//					type = DataSetType.FILE;
//					break;
//				case "dataset":
//					DataSet srcDs = initialContext.getDataSet(m_path);
//					if ( m_gcInfo == null && srcDs.hasGeometryColumn() ) {
//						m_gcInfo = srcDs.getGeometryColumnInfo();
//					}
//					m_path = srcDs.getHdfsPath();
//					type = DataSetType.LINK;
//					break;
//				case "gwave":
//					type = DataSetType.GWAVE;
//					break;
//				default:
//					throw new IllegalArgumentException("invalid dataset type: " + m_type);
//			}
//			
//			BindDataSetOptions opts = BindDataSetOptions.FORCE(m_force);
//			if ( m_gcInfo != null ) {
//				opts = opts.geometryColumnInfo(m_gcInfo);
//			}
//			initialContext.bindExternalDataSet(m_dataset, m_path, type, opts);
//		}
//	}
//
//	@Command(name="delete", description="delete the dataset(s)")
//	public static class Delete extends SubCommand<MarmotRuntime> {
//		@Parameters(paramLabel="id", index="0", arity="1..1", description={"dataset id"})
//		private String m_dsId;
//
//		@Option(names={"-r"}, description="list all descendant datasets")
//		private boolean m_recursive;
//
//		@Override
//		public void run(MarmotRuntime initialContext) throws Exception {
//			if ( m_recursive ) {
//				initialContext.deleteDir(m_dsId);
//			}
//			else {
//				initialContext.deleteDataSet(m_dsId);
//			}
//		}
//	}
//
//	@Command(name="attach_geometry", description="attach geometry data into the dataset")
//	public static class AttachGeometry extends SubCommand<MarmotRuntime> {
//		@Parameters(paramLabel="id", index="0", arity="1..1", description={"dataset id"})
//		private String m_dsId;
//
//		@Parameters(paramLabel="geometry_dataset", index="1", arity="1..1",
//					description={"geometry dataset id"})
//		private String m_geomDsId;
//
//		@Parameters(paramLabel="output_dataset", index="2", arity="1..1",
//					description={"output dataset id"})
//		private String m_outDsId;
//		
//		@Option(names={"-ref_col"}, paramLabel="column name", required=true,
//				description={"reference column in the dataset"})
//		private String m_refCol;
//		
//		@Option(names={"-key_col"}, paramLabel="column name", required=true,
//				description={"key column in the geometry dataset"})
//		private String m_keyCol;
//		
//		@Option(names={"-geom_col"}, paramLabel="column name", required=false,
//				description={"output geometry column name"})
//		private String m_geomCol = null;
//		
//		@Option(names={"-workers"}, paramLabel="worker count", required=false,
//				description={"join worker count"})
//		private FOption<Integer> m_nworkers = FOption.empty();
//
//		@Override
//		public void run(MarmotRuntime initialContext) throws Exception {
//			DataSet geomDs = initialContext.getDataSet(m_geomDsId);
//			if ( !geomDs.hasGeometryColumn() ) {
//				System.err.println("Geometry dataset does not have default Geometry column: "
//									+ "id=" + m_geomDsId);
//				System.exit(-1);
//			}
//			GeometryColumnInfo gcInfo = geomDs.getGeometryColumnInfo();
//
//			String outputGeomCol = (m_geomCol != null) ? m_geomCol : gcInfo.name();
//			JoinOptions opts = JoinOptions.INNER_JOIN(m_nworkers);
//
//			GeometryColumnInfo outGcInfo = new GeometryColumnInfo(outputGeomCol, gcInfo.srid());
//			String outputCols = String.format("param.%s as %s,*", gcInfo.name(), outputGeomCol);
//			Plan plan = Plan.builder("tag_geometry")
//									.load(m_dsId)
//									.hashJoin(m_refCol, m_geomDsId, m_keyCol, outputCols, opts)
//									.store(m_outDsId, FORCE(outGcInfo))
//									.build();
//			initialContext.execute(plan);
//		}
//	}
//
//	@Command(name="import",
//			subcommands= {
//				ImportCsvCmd.class,
//				ImportShapefileCmd.class,
//				ImportGeoJsonCmd.class,
//				ImportJdbcCmd.class
//			},
//			description="import into the dataset")
//	public static class Import extends SubCommand<MarmotRuntime> {
//		@Override
//		public void run(MarmotRuntime initialContext) throws Exception { }
//	}
//
//	@Command(name="csv", description="import CSV file into the dataset")
//	public static class ImportCsvCmd extends SubCommand<MarmotRuntime> {
//		@Mixin private CsvParameters m_csvParams;
//		@Mixin private ImportParameters m_params;
//		
//		@Parameters(paramLabel="file_path", index="0", arity="1..1",
//					description={"path to the target csv file"})
//		private String m_start;
//		
//		@Parameters(paramLabel="dataset_id", index="1", arity="1..1",
//					description={"dataset id to import onto"})
//		public void setDataSetId(String id) {
//			Utilities.checkNotNullArgument(id, "dataset id is null");
//			m_params.setDataSetId(id);
//		}
//		
//		@Option(names={"-glob"}, paramLabel="expr", description="glob expression for import files")
//		private String m_glob = "**/*.csv";
//
//		@Override
//		public void run(MarmotRuntime initialContext) throws Exception {
//			StopWatch watch = StopWatch.start();
//			
//			File csvFilePath = new File(m_start);
//			ImportIntoDataSet importFile = ImportCsv.from(csvFilePath, m_csvParams, m_params, m_glob);
//			importFile.getProgressObservable()
//						.subscribe(report -> {
//							double velo = report / watch.getElapsedInFloatingSeconds();
//							System.out.printf("imported: count=%d, elapsed=%s, velo=%.0f/s%n",
//											report, watch.getElapsedMillisString(), velo);
//						});
//			long count = importFile.run(initialContext);
//			
//			double velo = count / watch.getElapsedInFloatingSeconds();
//			System.out.printf("imported: dataset=%s count=%d elapsed=%s, velo=%.1f/s%n",
//							m_params.getDataSetId(), count, watch.getElapsedMillisString(), velo);
//		}
//	}
//
//	@Command(name="shp", aliases={"shapefile"}, description="import shapefile(s) into the dataset")
//	public static class ImportShapefileCmd extends SubCommand<MarmotRuntime> {
//		@Mixin private ImportParameters m_params;
//		@Mixin private ShapefileParameters m_shpParams;
//		
//		@Parameters(paramLabel="shp_file", index="0", arity="1..1",
//					description={"path to the target shapefile (or directory)"})
//		private String m_shpPath;
//		
//		@Parameters(paramLabel="dataset_id", index="1", arity="1..1",
//				description={"dataset id to import onto"})
//		public void setDataSetId(String id) {
//			Utilities.checkNotNullArgument(id, "dataset id is null");
//			m_params.setDataSetId(id);
//		}
//
//		@Override
//		public void run(MarmotRuntime initialContext) throws Exception {
//			StopWatch watch = StopWatch.start();
//			
//			if ( m_params.getGeometryColumnInfo().isAbsent() ) {
//				throw new IllegalArgumentException("Option '-geom_col' is missing");
//			}
//			
//			File shpFile = new File(m_shpPath);
//			ImportShapefile importFile = ImportShapefile.from(shpFile, m_shpParams, m_params);
//			importFile.getProgressObservable()
//						.subscribe(report -> {
//							double velo = report / watch.getElapsedInFloatingSeconds();
//							System.out.printf("imported: count=%d, elapsed=%s, velo=%.1f/s%n",
//											report, watch.getElapsedMillisString(), velo);
//						});
//			long count = importFile.run(initialContext);
//			
//			double velo = count / watch.getElapsedInFloatingSeconds();
//			System.out.printf("imported: dataset=%s count=%d elapsed=%s, velo=%.1f/s%n",
//								m_params.getDataSetId(), count, watch.getElapsedMillisString(),
//								velo);
//		}
//	}
//
//	@Command(name="geojson", description="import geojson file into the dataset")
//	public static class ImportGeoJsonCmd extends SubCommand<MarmotRuntime> {
//		@Mixin private GeoJsonParameters m_gjsonParams;
//		@Mixin private ImportParameters m_importParams;
//		
//		@Parameters(paramLabel="path", index="0", arity="1..1",
//					description={"path to the target geojson files (or directories)"})
//		private String m_path;
//
//		@Parameters(paramLabel="dataset_id", index="1", arity="1..1",
//				description={"dataset id to import onto"})
//		public void setDataSetId(String id) {
//			Utilities.checkNotNullArgument(id, "dataset id is null");
//			
//			m_importParams.setDataSetId(id);
//		}
//
//		@Override
//		public void run(MarmotRuntime initialContext) throws Exception {
//			StopWatch watch = StopWatch.start();
//			
//			if ( m_importParams.getGeometryColumnInfo().isAbsent() ) {
//				throw new IllegalArgumentException("Option '-geom_col' is missing");
//			}
//			
//			File gjsonFile = new File(m_path);
//			ImportGeoJson importFile = ImportGeoJson.from(gjsonFile, m_gjsonParams, m_importParams);
//			importFile.getProgressObservable()
//						.subscribe(report -> {
//							double velo = report / watch.getElapsedInFloatingSeconds();
//							System.out.printf("imported: count=%d, elapsed=%s, velo=%.1f/s%n",
//											report, watch.getElapsedMillisString(), velo);
//						});
//			long count = importFile.run(initialContext);
//			
//			double velo = count / watch.getElapsedInFloatingSeconds();
//			System.out.printf("imported: dataset=%s count=%d elapsed=%s, velo=%.1f/s%n",
//								m_importParams.getDataSetId(), count, watch.getElapsedMillisString(), velo);
//		}
//	}
//
//	@Command(name="jdbc", description="import a JDBC-connected table into a dataset")
//	public static class ImportJdbcCmd extends SubCommand<MarmotRuntime> {
//		@Mixin private LoadJdbcParameters m_jdbcParams;
//		@Mixin private ImportParameters m_importParams;
//
//		@Parameters(paramLabel="table_name", index="0", arity="1..1",
//					description={"JDBC table name"})
//		private String m_tableName;
//		
//		@Parameters(paramLabel="dataset_id", index="1", arity="1..1",
//				description={"dataset id to import onto"})
//		public void setDataSetId(String id) {
//			Utilities.checkNotNullArgument(id, "dataset id is null");
//			m_importParams.setDataSetId(id);
//		}
//
//		@Override
//		public void run(MarmotRuntime initialContext) throws Exception {
//			StopWatch watch = StopWatch.start();
//			
//			ImportJdbcTable importFile = ImportJdbcTable.from(m_tableName, m_jdbcParams,
//																m_importParams);
//			importFile.getProgressObservable()
//						.subscribe(report -> {
//							double velo = report / watch.getElapsedInFloatingSeconds();
//							System.out.printf("imported: count=%d, elapsed=%s, velo=%.1f/s%n",
//											report, watch.getElapsedMillisString(), velo);
//						});
//			long count = importFile.run(initialContext);
//			
//			double velo = count / watch.getElapsedInFloatingSeconds();
//			System.out.printf("imported: dataset=%s count=%d elapsed=%s, velo=%.1f/s%n",
//								m_importParams.getDataSetId(), count, watch.getElapsedMillisString(), velo);
//		}
//	}
//	
//	@Command(name="export",
//			subcommands= {
//				ExportCsv.class,
//				ExportShapefile.class,
//				ExportGeoJson.class,
//				ExportJdbcTable.class,
//			},
//			description="export a dataset")
//	public static class Export extends SubCommand<MarmotRuntime> {
//		@Override
//		public void run(MarmotRuntime initialContext) throws Exception { }
//	}
//	
//	private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
//
//	@Command(name="csv", description="export a dataset in CSV format")
//	public static class ExportCsv extends SubCommand<MarmotRuntime> {
//		@Parameters(paramLabel="dataset_id", index="0", arity="1..1",
//					description={"dataset id to export"})
//		private String m_dsId;
//
//		@Parameters(paramLabel="file_path", index="1", arity="0..1",
//					description={"file path for exported CSV file"})
//		private String m_output;
//
//		@Mixin private CsvParameters m_csvParams;
//		
//		@Option(names={"-f"}, description="delete the file if it exists already")
//		private boolean m_force;
//
//		@Override
//		public void run(MarmotRuntime initialContext) throws Exception {
//			m_csvParams.charset().ifAbsent(() -> m_csvParams.charset(DEFAULT_CHARSET));
//			
//			File outFile = new File(m_output);
//			if ( m_force && m_output != null && outFile.exists() ) {
//				FileUtils.forceDelete(outFile);
//			}
//			
//			FOption<String> output = FOption.ofNullable(m_output);
//			BufferedWriter writer = ExternIoUtils.toWriter(output, m_csvParams.charset().get());
//			new ExportAsCsv(m_dsId, m_csvParams).run(initialContext, writer);
//		}
//	}
//
//	@Command(name="shp", description="export the dataset in Shapefile format")
//	public static class ExportShapefile extends SubCommand<MarmotRuntime> {
//		@Mixin private ExportShapefileParameters m_shpParams;
//		
//		@Parameters(paramLabel="dataset_id", index="0", arity="1..1",
//					description={"dataset id to export"})
//		private String m_dsId;
//
//		@Parameters(paramLabel="output_dir", index="1", arity="1..1",
//					description={"directory path for the output shapefiles"})
//		private String m_output;
//		
//		@Option(names={"-f", "-force"}, description="force to create a new output directory")
//		private boolean m_force;
//		
//		@Option(names={"-report_interval"}, paramLabel="record count",
//				description="progress report interval")
//		private int m_interval = -1;
//		
//		@Override
//		public void run(MarmotRuntime initialContext) throws Exception {
//			ExportDataSetAsShapefile export = new ExportDataSetAsShapefile(m_dsId, m_output,
//																			m_shpParams);
//			export.setForce(m_force);
//			FOption.when(m_interval > 0, m_interval)
//					.ifPresent(export::setProgressInterval);
//			
//			ProgressiveExecution<Long, Long> act = export.start(initialContext);
//			act.get();
//		}
//	}
//
//	@Command(name="geojson", description="export a dataset in GeoJSON format")
//	public static class ExportGeoJson extends SubCommand<MarmotRuntime> {
//		@Mixin private GeoJsonParameters m_gjsonParams;
//		
//		@Parameters(paramLabel="dataset_id", index="0", arity="1..1",
//					description={"dataset id to export"})
//		private String m_dsId;
//
//		@Parameters(paramLabel="file_path", index="1", arity="0..1",
//					description={"file path for exported GeoJson file"})
//		private String m_output;
//		
//		@Option(names={"-p", "-pretty"}, description={"path to the output CSV file"})
//		private boolean m_pretty;
//
//		@Override
//		public void run(MarmotRuntime initialContext) throws Exception {
//			ExportAsGeoJson export = new ExportAsGeoJson(m_dsId)
//										.printPrinter(m_pretty);
//			m_gjsonParams.geoJsonSrid().ifPresent(export::setGeoJSONSrid);
//			
//			FOption<String> output = FOption.ofNullable(m_output);
//			BufferedWriter writer = ExternIoUtils.toWriter(output, m_gjsonParams.charset());
//			long count = export.run(initialContext, writer);
//			
//			System.out.printf("done: %d records%n", count);
//		}
//	}
//
//	@Command(name="jdbc", description="export a dataset into JDBC table")
//	public static class ExportJdbcTable extends SubCommand<MarmotRuntime> {
//		@Mixin private StoreJdbcParameters m_jdbcParams;
//		
//		@Parameters(paramLabel="dataset_id", index="0", arity="1..1",
//					description={"dataset id to export"})
//		private String m_dsId;
//
//		@Parameters(paramLabel="table_name", index="1", arity="1..1",
//					description={"JDBC table name"})
//		private String m_tblName;
//		
//		@Option(names={"-report_interval"}, paramLabel="record count",
//				description="progress report interval")
//		private int m_interval = -1;
//
//		@Override
//		public void run(MarmotRuntime initialContext) throws Exception {
//			ExportIntoJdbcTable export = new ExportIntoJdbcTable(m_dsId, m_tblName, m_jdbcParams);
//			FOption.when(m_interval > 0, m_interval)
//					.ifPresent(export::reportInterval);
//			
//			long count = export.run(initialContext);
//			System.out.printf("done: %d records%n", count);
//		}
//	}
//
//	@Command(name="thumbnail",
//			subcommands= {
//				CreateThumbnail.class, DeleteThumbnail.class,
//			},
//			description="thumbnail related commands")
//	public static class Thumbnail extends SubCommand<MarmotRuntime> {
//		@Override
//		public void run(MarmotRuntime initialContext) throws Exception { }
//	}
//
//	@Command(name="create", description="create a thumbnail for a dataset")
//	public static class CreateThumbnail extends SubCommand<MarmotRuntime> {
//		@Parameters(paramLabel="dataset", index="0", arity="1..1",
//					description={"dataset id for thumbnail"})
//		private String m_dsId;
//
//		@Parameters(paramLabel="sample_count", index="1", arity="1..1",
//					description={"sample count"})
//		private long m_sampleCount;
//
//		@Override
//		public void run(MarmotRuntime initialContext) throws Exception {
//			StopWatch watch = StopWatch.start();
//			
//			DataSet ds = initialContext.getDataSet(m_dsId);
//			ds.createThumbnail((int)m_sampleCount);
//			
//			System.out.printf("nsmaples=%,d, elapsed time: %s%n",
//							m_sampleCount, watch.stopAndGetElpasedTimeString());
//		}
//	}
//
//	@Command(name="delete", description="delete a thumbnail for a dataset")
//	public static class DeleteThumbnail extends SubCommand<MarmotRuntime> {
//		@Parameters(paramLabel="dataset", index="0", arity="1..1",
//					description={"dataset id for thumbnail"})
//		private String m_dsId;
//
//		@Override
//		public void run(MarmotRuntime initialContext) throws Exception {
//			DataSet ds = initialContext.getDataSet(m_dsId);
//			ds.deleteThumbnail();
//		}
//	}
}
