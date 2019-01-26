package edu.student;

import storageManager.FieldType;

public class TupleValue {
    public int number;
    public String str;
    public FieldType fieldType;

    public boolean equals(TupleValue tvalue) {
        if (this.fieldType == tvalue.fieldType) {
            if (this.fieldType == FieldType.INT) {
                return this.number == tvalue.number;
            } else {
                return this.str.equals(tvalue.str);
            }
        } else {
            return false;
        }
    }
}

