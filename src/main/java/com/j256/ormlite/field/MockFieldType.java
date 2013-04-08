package com.j256.ormlite.field;

import com.j256.ormlite.support.ConnectionSource;

import java.lang.reflect.Field;
import java.sql.SQLException;

public class MockFieldType extends FieldType {

    public static final String FOREIGN_CLASS_FIELD_SUFFIX = "_class";

    /* This field tricks existing ormLite as it only uses fields for data types and names.
     * I know it is a dirty hack but it's the only way I found to do it.
     */
    @DatabaseField()
    private String foreignClassMock;

    private FieldType baseFieldType;

    public static Field getMockForeignField(){
        try{
            return MockFieldType.class.getDeclaredField("foreignClassMock");
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }

    public MockFieldType(ConnectionSource connectionSource, String tableName, DatabaseFieldConfig fieldConfig, Class<?> parentClass, FieldType baseFieldType) throws SQLException {
        super(connectionSource, tableName, MockFieldType.getMockForeignField(), fieldConfig, parentClass);
        this.baseFieldType = baseFieldType;

        String idName = baseFieldType.getColumnName();
        this.columnName = idName.substring(0,idName.length()-FOREIGN_ID_FIELD_SUFFIX.length()) + FOREIGN_CLASS_FIELD_SUFFIX;
    }

    @Override
    public String getFieldName() {
        return super.getFieldName();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <FV> FV extractRawJavaFieldValue(Object object) throws SQLException {
        Object o = this.baseFieldType.extractRawJavaFieldValue(object);
        return  (FV)o.getClass().getName();
    }

}
