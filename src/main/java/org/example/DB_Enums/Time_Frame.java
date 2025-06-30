package org.example.DB_Enums;

import org.example.StringResources;

public enum Time_Frame {
    FIVE_MINUTE {
        @Override
        public String getDatabaseAbbreviation() {
            return StringResources.FIVE_MINUTE_ABBREVIATION;
        }
    },
    FIFTEEN_MINUTE {
        @Override
        public String getDatabaseAbbreviation() {
            return StringResources.FIFTEEN_MINUTE_ABBREVIATION;
        }
    },
    THIRTY_MINUTE {
        @Override
        public String getDatabaseAbbreviation() {
            return StringResources.THIRTY_MINUTE_ABBREVIATION;
        }
    };

    public abstract String getDatabaseAbbreviation();
}
