package com.rules.model;

public class Rules {


public String getID() {
		return ID;
	}
	public void setID(String iD) {
		ID = iD;
	}
	public String getInterestSegment() {
		return interestSegment;
	}
	public void setInterestSegment(String interestSegment) {
		this.interestSegment = interestSegment;
	}
	public String getGender() {
		return gender;
	}
	public void setGender(String gender) {
		this.gender = gender;
	}
	public String getAgeGroup() {
		return ageGroup;
	}
	public void setAgeGroup(String ageGroup) {
		this.ageGroup = ageGroup;
	}
	public String getIncomeLevel() {
		return incomeLevel;
	}
	public void setIncomeLevel(String incomeLevel) {
		this.incomeLevel = incomeLevel;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	
	public String getRuleName() {
		return ruleName;
	}
	
	public void setRuleName(String ruleName) {
		this.ruleName = ruleName;
	}
	
	
	
	
private String ID;
private String ruleName;
private String interestSegment;
private String gender;
private String ageGroup;
private String incomeLevel;
private String status;






}
