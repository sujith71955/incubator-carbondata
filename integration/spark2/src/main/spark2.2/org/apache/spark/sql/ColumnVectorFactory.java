package org.apache.spark.sql;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class ColumnVectorFactory {


    public static WritableColumnVector[] getColumnVector(MemoryMode memMode, StructType outputSchema, int rowNums) {


        WritableColumnVector[] writableColumnVectors = null;
        switch (memMode) {
            case ON_HEAP:
                writableColumnVectors =OnHeapColumnVector
                        .allocateColumns(rowNums, outputSchema);
                break;
            case OFF_HEAP:
                writableColumnVectors = OffHeapColumnVector
                        .allocateColumns(rowNums, outputSchema);
                break;
        }
        return writableColumnVectors;
    }
}
