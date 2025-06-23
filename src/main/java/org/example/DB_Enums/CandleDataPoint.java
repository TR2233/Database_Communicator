package org.example.DB_Enums;

//these enums MUST match the column names in the google cloud database tables
public enum CandleDataPoint {
    TICKER,
    TIMESTAMP,
    HIGH,
    OPEN,
    LOW,
    CLOSE,
    PREVIOUS_CLOSE,
    VOLUME;
}
