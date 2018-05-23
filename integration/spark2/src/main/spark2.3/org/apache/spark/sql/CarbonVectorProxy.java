package org.apache.spark.sql;

import org.apache.spark.sql.ColumnVectorFactory;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

import java.math.BigDecimal;
import java.math.BigInteger;

public class CarbonVectorProxy {

    private static final int DEFAULT_BATCH_SIZE = 4 * 1024;
    private WritableColumnVector columnVector;
    private ColumnarBatch columnarBatch;
    private WritableColumnVector[] writableColumnVectors;

    public CarbonVectorProxy(MemoryMode memMode, int rowNum, StructField[] structFileds) {
        writableColumnVectors = ColumnVectorFactory
                .getColumnVector(MemoryMode.OFF_HEAP, new StructType(structFileds), DEFAULT_BATCH_SIZE);
        columnarBatch = new ColumnarBatch(writableColumnVectors);
    }
    
    public CarbonVectorProxy(MemoryMode memMode, int rowNum, StructType outputSchema) {
        writableColumnVectors = ColumnVectorFactory
                .getColumnVector(MemoryMode.OFF_HEAP,  outputSchema, DEFAULT_BATCH_SIZE);
        columnarBatch = new ColumnarBatch(writableColumnVectors);
    }

    public int numRows() {
        return columnarBatch.numRows();
    }

    public ColumnVector column(int ordinal) {
        return columnarBatch.column(ordinal);
    }

    public void reset() {
        for (WritableColumnVector col : writableColumnVectors) {
            col.reset();
        }
    }

    public InternalRow getRow(int rowId) {
        return columnarBatch.getRow(rowId);
    }


    /**
     * Returns the row in this batch at `rowId`. Returned row is reused across calls.
     */
    public ColumnarBatch getColumnarBatch() {
        return columnarBatch;
    }

    public void close() {
        columnarBatch.close();
    }

    /**
     * Sets the number of rows in this batch.
     */
    public void setNumRows(int numRows) {
        columnarBatch.setNumRows(numRows);
    }


    public void putRowToColumnBatch(ColumnVector colVector, int rowId, Object value) {
        if (colVector instanceof WritableColumnVector) {
            if (colVector instanceof WritableColumnVector) {
                this.columnVector = (WritableColumnVector) columnVector;
            }
            org.apache.spark.sql.types.DataType t = columnVector.dataType();
            if (null == value) {
                columnVector.putNull(rowId);
            } else {
                if (t == org.apache.spark.sql.types.DataTypes.BooleanType) {
                    columnVector.putBoolean(rowId, (boolean) value);
                } else if (t == org.apache.spark.sql.types.DataTypes.ByteType) {
                    columnVector.putByte(rowId, (byte) value);
                } else if (t == org.apache.spark.sql.types.DataTypes.ShortType) {
                    columnVector.putShort(rowId, (short) value);
                } else if (t == org.apache.spark.sql.types.DataTypes.IntegerType) {
                    columnVector.putInt(rowId, (int) value);
                } else if (t == org.apache.spark.sql.types.DataTypes.LongType) {
                    columnVector.putLong(rowId, (long) value);
                } else if (t == org.apache.spark.sql.types.DataTypes.FloatType) {
                    columnVector.putFloat(rowId, (float) value);
                } else if (t == org.apache.spark.sql.types.DataTypes.DoubleType) {
                    columnVector.putDouble(rowId, (double) value);
                } else if (t == org.apache.spark.sql.types.DataTypes.StringType) {
                    UTF8String v = (UTF8String) value;
                    columnVector.putByteArray(rowId, v.getBytes());
                } else if (t instanceof DecimalType) {
                    DecimalType dt = (DecimalType) t;
                    Decimal d = Decimal.fromDecimal(value);
                    if (dt.precision() <= Decimal.MAX_INT_DIGITS()) {
                        columnVector.putInt(rowId, (int) d.toUnscaledLong());
                    } else if (dt.precision() <= Decimal.MAX_LONG_DIGITS()) {
                        columnVector.putLong(rowId, d.toUnscaledLong());
                    } else {
                        final BigInteger integer = d.toJavaBigDecimal().unscaledValue();
                        byte[] bytes = integer.toByteArray();
                        columnVector.putByteArray(rowId, bytes, 0, bytes.length);
                    }
                } else if (t instanceof CalendarIntervalType) {
                    CalendarInterval c = (CalendarInterval) value;
                    columnVector.getChild(0).putInt(rowId, c.months);
                    columnVector.getChild(1).putLong(rowId, c.microseconds);
                } else if (t instanceof org.apache.spark.sql.types.DateType) {
                    columnVector.putInt(rowId, (int) value);
                } else if (t instanceof org.apache.spark.sql.types.TimestampType) {
                    columnVector.putLong(rowId, (long) value);
                }
            }
        }
    }

    public void putBoolean(int rowId, boolean value) {

        columnVector.putBoolean(rowId, (boolean) value);
    }

    public void putByte(int rowId, byte value) {

        columnVector.putByte(rowId, (byte) value);
    }

    public void putShort(int rowId, short value) {

        columnVector.putShort(rowId, (short) value);
    }

    public void putInt(int rowId, int value) {

        columnVector.putInt(rowId, (int) value);
    }

    public void putFloat(int rowId, float value) {

        columnVector.putFloat(rowId, (float) value);
    }

    public void putLong(int rowId, long value) {

        columnVector.putLong(rowId, (long) value);
    }

    public void putDouble(int rowId, double value) {

        columnVector.putDouble(rowId, (double) value);
    }

    public void putByteArray(int rowId, byte[] value) {

        columnVector.putByteArray(rowId, (byte[]) value);
    }

    public void putInts(int rowId, int count, int value) {

        columnVector.putInts(rowId, count, value);
    }

    public void putShorts(int rowId, int count, short value) {

        columnVector.putShorts(rowId, count, value);
    }

    public void putLongs(int rowId, int count, long value) {

        columnVector.putLongs(rowId, count, value);
    }

    public void putDecimal(int rowId, Decimal value, int precision) {
        columnVector.putDecimal(rowId, value, precision);

    }

    public void putDoubles(int rowId, int count, double value) {

        columnVector.putDoubles(rowId, count, value);
    }

    public void putByteArray(int rowId, byte[] value, int offset, int length) {

        columnVector.putByteArray(rowId, (byte[]) value, offset, length);
    }

    public void putNull(int rowId) {

        columnVector.putNull(rowId);
    }

    public void putNulls(int rowId, int count) {

        columnVector.putNulls(rowId, count);
    }

    public boolean isNullAt(int rowId) {

        return columnVector.isNullAt(rowId);
    }

    public DataType dataType() {

        return columnVector.dataType();
    }


}
