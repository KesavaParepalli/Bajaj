package com.bajaj.challenge;

import com.bajaj.challenge.dto.RequestDTO;
import com.bajaj.challenge.dto.ResponseDTO;
import com.bajaj.challenge.dto.SubmissionDTO;
import org.springframework.boot.CommandLineRunner;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class Runner implements CommandLineRunner {

    private final RestTemplate restTemplate = new RestTemplate();

    @Override
    public void run(String... args) throws Exception {
        String registrationUrl = "https://bfhldevapigw.healthrx.co.in/hiring/generateWebhook/JAVA";

        RequestDTO registrationRequest = new RequestDTO(
                "Parepalli Kesava Naga Sai Durga Vardhan",
                "22BCE20530",
                "kesavaparepalli@gmail.com"
        );

        System.out.println("Sending registration request to: " + registrationUrl);
        System.out.println("Request body: " + registrationRequest.toString());

        try {
            HttpHeaders regHeaders = new HttpHeaders();
            regHeaders.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<RequestDTO> regEntity = new HttpEntity<>(registrationRequest, regHeaders);

            ResponseEntity<ResponseDTO> regResponse = restTemplate.exchange(
                    registrationUrl,
                    HttpMethod.POST,
                    regEntity,
                    ResponseDTO.class
            );

            ResponseDTO webhookResponse = regResponse.getBody();

            System.out.println("Registration API Status: " + regResponse.getStatusCode());
            System.out.println("Parsed response: " + webhookResponse);

            if (webhookResponse == null) {
                System.err.println("Webhook response is completely null. API call failed.");
                return;
            }

            String webhookUrl = webhookResponse.getWebhookUrl();
            String accessToken = webhookResponse.getAccessToken();

            System.out.println("Webhook URL: " + webhookUrl);
            System.out.println("Access Token: " + (accessToken != null ? "***TOKEN_RECEIVED***" : "null"));

            if (webhookUrl == null || webhookUrl.trim().isEmpty()) {
                System.err.println("Webhook URL is null or empty. Cannot proceed with submission.");
                return;
            }

            if (accessToken == null || accessToken.trim().isEmpty()) {
                System.err.println("Access token is null or empty. Cannot proceed with submission.");
                return;
            }


            
            String finalQuery =
                "SELECT\n" +
                "    department_name AS DEPARTMENT_NAME,\n" +
                "    AVG(age) AS AVERAGE_AGE,\n" +
                "    STRING_AGG(first_name || ' ' || last_name, ', ' ORDER BY first_name, last_name)\n" +
                "        WITHIN GROUP (ORDER BY first_name, last_name)\n" +
                "        || '' AS EMPLOYEE_LIST\n" +
                "FROM (\n" +
                "    SELECT\n" +
                "        e.*,\n" +
                "        ROW_NUMBER() OVER (\n" +
                "            PARTITION BY department_id\n" +
                "            ORDER BY first_name, last_name\n" +
                "        ) AS rn\n" +
                "    FROM employees e\n" +
                "    WHERE salary > 70000\n" +
                ") t\n" +
                "WHERE rn <= 10\n" +
                "GROUP BY department_id, department_name\n" +
                "ORDER BY department_id DESC;\n";
            

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.set("Authorization", accessToken);

            SubmissionDTO submissionRequest = new SubmissionDTO(finalQuery);

            HttpEntity<SubmissionDTO> entity = new HttpEntity<>(submissionRequest, headers);

            System.out.println("Submitting solution to webhook: " + webhookUrl);

            ResponseEntity<String> response = restTemplate.exchange(
                    webhookUrl,
                    HttpMethod.POST,
                    entity,
                    String.class
            );

            String submissionResponse = response.getBody();
            System.out.println("Submission successful! Response: " + submissionResponse);
            System.out.println("HTTP Status: " + response.getStatusCode());

        } catch (Exception e) {
            System.err.println("Error during execution: " + e.getMessage());
            e.printStackTrace();
        }
    }
}