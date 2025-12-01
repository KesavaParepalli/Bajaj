package com.bajaj.challenge.dto;

import lombok.Data;
import com.fasterxml.jackson.annotation.JsonProperty;

@Data
public class ResponseDTO {
    @JsonProperty("webhook")
    private String webhookUrl;
    private String accessToken;
}