package com.hue.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.hue.common.ColumnKeyType;
import com.hue.common.DataType;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Column extends HueBase {
	private static final long serialVersionUID = 1L;

	private DataType dataType = DataType.STRING ;
	private ColumnKeyType keyType = ColumnKeyType.NO_KEY_TYPE;

	public Column() {
	}

	public Column(String name, ColumnKeyType type, DataType dataType){
		setName(name);
		setKeyType(type);
		setDataType(dataType);
	}

	public DataType getDataType() {
		return this.dataType;
	}

	public void setDataType(DataType dataType) {
		this.dataType = dataType;
	}

	public ColumnKeyType getKeyType() {
		return this.keyType;
	}

	public void setKeyType(ColumnKeyType keyType) {
		this.keyType = keyType;
	}

	@Override
	public Object clone() {
		Column column = new Column();

		column.setDataType(getDataType());
		column.setKeyType(getKeyType());
		column.setName(getName());

		return column;
	}
}
