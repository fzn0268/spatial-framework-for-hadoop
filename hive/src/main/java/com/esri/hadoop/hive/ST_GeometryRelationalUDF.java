package com.esri.hadoop.hive;

import com.esri.core.geometry.OperatorSimpleRelation;
import com.esri.core.geometry.ogc.OGCGeometry;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;

/**
 * Abstract class that all simple relational tests (contains, touches, ...) extend from
 *
 */
public abstract class ST_GeometryRelationalUDF extends UDF {
    private static final Log LOG = LogFactory.getLog(ST_GeometryRelationalUDF.class);

    private static final BooleanWritable FALSE = new BooleanWritable(false);

    /**
     * Operators that extend this should return an instance of
     * <code>OperatorSimpleRelation</code>
     *
     * @return operator for simple relationship tests
     */
    protected abstract OperatorSimpleRelation getRelationOperator();

    public BooleanWritable evaluate(BytesWritable geom1ref, BytesWritable geom2ref) {

        if (geom1ref == null || geom2ref == null) {
            LogUtils.Log_ArgumentsNull(LOG);
            return FALSE;
        }

        OGCGeometry geom1 = GeometryUtils.geometryFromEsriShape(geom1ref);
        OGCGeometry geom2 = GeometryUtils.geometryFromEsriShape(geom2ref);

        return new BooleanWritable(getRelationOperator().execute(geom1.getEsriGeometry(), geom2.getEsriGeometry(), geom1.getEsriSpatialReference(), null));
    }
}
