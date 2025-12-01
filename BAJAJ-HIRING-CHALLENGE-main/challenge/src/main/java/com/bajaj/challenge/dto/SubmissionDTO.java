package com.bajaj.challenge.dto;

import lombok.Data;

@Data
public class SubmissionDTO {
    private String finalQuery;

    public SubmissionDTO(String finalQuery) {
        this.finalQuery = finalQuery;
    }
}
