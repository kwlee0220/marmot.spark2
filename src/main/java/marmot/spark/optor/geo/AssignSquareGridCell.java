package marmot.spark.optor.geo;

import java.util.List;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;

import com.google.common.collect.Lists;

import marmot.GRecordSchema;
import marmot.RecordSchema;
import marmot.geo.GeoClientUtils;
import marmot.spark.MarmotSpark;
import marmot.spark.RecordLite;
import marmot.spark.optor.RecordLevelRDDFlatFunction;
import marmot.type.DataType;
import marmot.type.GridCell;
import utils.Size2d;
import utils.Size2i;
import utils.Utilities;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AssignSquareGridCell extends RecordLevelRDDFlatFunction {
	private static final long serialVersionUID = 1L;
	
	private final SquareGrid m_grid;
	private final boolean m_assignOutside;

	private Envelope m_bounds;
	private Size2i m_gridSize;
	private int m_geomColIdx = -1;
	private Polygon m_universePolygon;
	private int m_outputColIdx;
	
	public AssignSquareGridCell(SquareGrid grid, boolean assignOutside) {
		super(true);
		
		Utilities.checkNotNullArgument(grid, "SquareGrid is null");
		
		m_grid = grid;
		m_assignOutside = assignOutside;
	}

	@Override
	protected GRecordSchema _initialize(MarmotSpark marmot, GRecordSchema inputGSchema) {
		m_bounds = m_grid.getGridBounds(marmot);
		Size2d cellSize = m_grid.getCellSize();
		m_gridSize = new Size2i((int)Math.ceil(m_bounds.getWidth() / cellSize.getWidth()),
								(int)Math.ceil(m_bounds.getHeight() / cellSize.getHeight()));
		
		RecordSchema schema = inputGSchema.getRecordSchema();
		m_geomColIdx = inputGSchema.getGeometryColumnIdx();
		
		m_universePolygon = GeoClientUtils.toPolygon(m_bounds);
		m_outputColIdx = schema.getColumnCount();
		
		RecordSchema outSchema = schema.toBuilder()
										.addColumn("cell_geom", DataType.POLYGON)
										.addColumn("cell_pos", DataType.GRID_CELL)
										.addColumn("cell_id", DataType.LONG)
										.build();
		return inputGSchema.derive(outSchema);
	}

	@Override
	protected void buildContext() {
	}

	@Override
	protected FStream<RecordLite> flatMapRecord(RecordLite input) {
		Geometry geom = input.getGeometry(m_geomColIdx);
		if ( geom != null && !geom.isEmpty() && m_universePolygon.intersects(geom) ) {
			return FStream.from(findCover(input))
							.map(info -> {
								RecordLite record = RecordLite.of(getRecordSchema());
								record.set(input);
								record.set(m_outputColIdx, info.m_geom);
								record.set(m_outputColIdx+1, info.m_pos);
								record.set(m_outputColIdx+2, info.m_ordinal);
								
								return record;
							});
		}
		else if ( !m_assignOutside ) {
			return FStream.empty();
		}
		else {
			RecordLite output = RecordLite.of(getRecordSchema());
			output.set(input);
			
			return FStream.of(output);
		}
	}
	
	@Override
	public String toString() {
		return String.format("%s: geom=%s, grid=%s", getClass().getSimpleName(),
							m_inputGSchema.assertGeometryColumnInfo(), m_grid);
	}
	
	private static class CellInfo {
		private final Geometry m_geom;
		private final GridCell m_pos;
		private final long m_ordinal;
		
		private CellInfo(Geometry geom, GridCell pos, long ordinal) {
			m_geom = geom;
			m_pos = pos;
			m_ordinal = ordinal;
		}
		
		@Override
		public String toString() {
			return String.format("%s:%d", m_pos, m_ordinal);
		}
	}
	
	private List<CellInfo> findCover(RecordLite record) {
		Geometry geom = record.getGeometry(m_geomColIdx);
		Envelope envl = geom.getEnvelopeInternal();
		double width = m_grid.getCellSize().getWidth();
		double height = m_grid.getCellSize().getHeight();
		
		int minX = (int)Math.floor((envl.getMinX() - m_bounds.getMinX()) / width);
		int minY = (int)Math.floor((envl.getMinY() - m_bounds.getMinY()) / height);
		int maxX = (int)Math.floor((envl.getMaxX() - m_bounds.getMinX()) / width);
		int maxY = (int)Math.floor((envl.getMaxY() - m_bounds.getMinY()) / height);
		
		List<CellInfo> cover = Lists.newArrayList();
		for ( int y = minY; y <= maxY; ++y ) {
			for ( int x = minX; x <= maxX; ++x ) {
				double x1 = m_bounds.getMinX() + (x * width);
				double y1 = m_bounds.getMinY() + (y * height);
				Envelope cellEnvl = new Envelope(x1, x1 + width, y1, y1 + height);
				Polygon poly = GeoClientUtils.toPolygon(cellEnvl);
				if ( poly.intersects(geom) ) {
					long ordinal = y * (m_gridSize.getWidth()) + x;
					
					cover.add(new CellInfo(poly, new GridCell(x,y), ordinal));
				}
			}
		}
		
		return cover;
	}
}
