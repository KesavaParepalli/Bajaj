package com.bajaj.challenge.dto;

import lombok.Data;

@Data
public class RequestDTO {
    private String name;
    private String regNo;
    private String email;

    public RequestDTO(String name, String regNo, String email) {
        this.name = name;
        this.regNo = regNo;
        this.email = email;
    }

    @Override
    public String toString() {
        return "RequestDTO{" +
                "name='" + name + '\'' +
                ", regNo='" + regNo + '\'' +
                ", email='" + email + '\'' +
                '}';
    }
}