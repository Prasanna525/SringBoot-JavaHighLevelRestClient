package com.prasanna.JavaHighLevelRestClient.index;

//import lombok.Data;
//
//@Data
public class Technologies {
	
	private String name;
	private String yearsOfExperience;
	
	public Technologies() {
		
	}
	
	public Technologies(String name, String yearsOfExperience) {
		super();
		this.name = name;
		this.yearsOfExperience = yearsOfExperience;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getYearsOfExperience() {
		return yearsOfExperience;
	}
	public void setYearsOfExperience(String yearsOfExperience) {
		this.yearsOfExperience = yearsOfExperience;
	}
	
	

}
