//package edu.utdallas.davisbase;


import java.util.ArrayList;

public class Table {
	
	public String tbl_name;
	
	public int num_cols;
	public ArrayList<String> tbls;
	public ArrayList<String> cols;
	public ArrayList<String> dataTypes;
	public ArrayList<Boolean> is_nullable;
	public ArrayList<Boolean> unique;
	
	public String getTbl_name() {
		return tbl_name;
	}
	public void setTbl_name(String tbl_name) {
		this.tbl_name = tbl_name;
	}
	public int getNum_cols() {
		return num_cols;
	}
	public void setNum_cols(int num_cols) {
		this.num_cols = num_cols;
	}
	
	public ArrayList<String> gettbls() {
                return tbls;
        }

        public void settbls(ArrayList<String> tbls){
                this.tbls = tbls;
        }

	public ArrayList<String> getCols() {
		return cols;
	}
	public void setCols(ArrayList<String> cols) {
		this.cols = cols;
	}
	public ArrayList<String> getDataTypes() {
		return dataTypes;
	}
	public void setDataTypes(ArrayList<String> dataTypes) {
		this.dataTypes = dataTypes;
	}
	public ArrayList<Boolean> getIs_nullable() {
		return is_nullable;
	}
	public void setIs_nullable(ArrayList<Boolean> is_nullable) {
		this.is_nullable = is_nullable;
	}
	public ArrayList<Boolean> getUnique() {
		return unique;
	}
	public void setUnique(ArrayList<Boolean> unique) {
		this.unique = unique;
	}
	
	
}
