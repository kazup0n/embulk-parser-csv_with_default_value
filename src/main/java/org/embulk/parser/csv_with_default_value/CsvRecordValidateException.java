package org.embulk.parser.csv_with_default_value;

import org.embulk.spi.DataException;

/**
 * Created by k.sasaki on 2016/10/25.
 */
class CsvRecordValidateException
        extends DataException {
    CsvRecordValidateException(Throwable cause) {
        super(cause);
    }
}
