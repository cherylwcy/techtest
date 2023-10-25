package com.db.dataplatform.techtest.client.component.impl;

import com.db.dataplatform.techtest.client.api.model.DataEnvelope;
import com.db.dataplatform.techtest.client.component.Client;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriTemplate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Client code does not require any test coverage
 */

@Service
@Slf4j
@RequiredArgsConstructor
public class   ClientImpl implements Client {

    public static final String URI_PUSHDATA = "http://localhost:8090/dataserver/pushdata";
    public static final UriTemplate URI_GETDATA = new UriTemplate("http://localhost:8090/dataserver/data/{blockType}");
    public static final UriTemplate URI_PATCHDATA = new UriTemplate("http://localhost:8090/dataserver/update/{name}/{newBlockType}");

    @Override
    public void pushData(DataEnvelope dataEnvelope) {
        log.info("Pushing data {} to {}", dataEnvelope.getDataHeader().getName(), URI_PUSHDATA);
        try {
            ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
            String json = ow.writeValueAsString(dataEnvelope);
            log.debug("Data block: {}", json);
            String md5hex = DigestUtils.md5Hex(dataEnvelope.getDataBody().getDataBody());
            log.debug("Client MD5: {}",md5hex);

            HttpHeaders headers = new HttpHeaders();
            headers.add("Accept", "application/json");
            headers.add("Content-type", "application/json");
            headers.add("Content-MD5", md5hex);

            HttpEntity<DataEnvelope> entity = new HttpEntity<>(dataEnvelope, headers);
            RestTemplate restTemplate = new RestTemplate();
            ResponseEntity<Boolean> response = restTemplate.postForEntity(URI_PUSHDATA, entity, Boolean.class);
            log.info("PushData {}: {}", dataEnvelope.getDataHeader().getName(), Boolean.TRUE.equals(response.getBody()) ? "Success" : "Failed");
        }
        catch (Exception e) {
            log.error("Exception", e);
        }
    }

    @Override
    public List<DataEnvelope> getData(String blockType) {

        Map<String, String> uriVariables = new HashMap<>();
        uriVariables.put("blockType", blockType);
        log.info("Querying by {}", URI_GETDATA.expand(uriVariables));

        RestTemplate restTemplate = new RestTemplate();

        @SuppressWarnings("rawtypes")
        ResponseEntity<List> response =
                restTemplate.getForEntity(URI_GETDATA.expand(uriVariables),
                        List.class);

        @SuppressWarnings("unchecked")
        List<DataEnvelope> dataEnvelopeList = response.getBody();

        if (dataEnvelopeList != null && !dataEnvelopeList.isEmpty()) {
            try {
                ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
                String json = ow.writeValueAsString(dataEnvelopeList);
                log.info("dataEnvelopeList: " + json);
            } catch (Exception e) {
                log.info("Cannot convert to JSON", e);
            }
        }
        return dataEnvelopeList;
    }

    @Override
    public boolean updateData(String blockName, String newBlockType) {

        Map<String, String> uriVariables = new HashMap<>();
        uriVariables.put("name", blockName);
        uriVariables.put("newBlockType", newBlockType);
        log.info("Updating by {}", URI_PATCHDATA.expand(uriVariables));

        RestTemplate restTemplate = new RestTemplate();
        ResponseEntity<Boolean> response = restTemplate.getForEntity(URI_PATCHDATA.expand(uriVariables), Boolean.class);
        log.info("Update data {} to new block type {}: {}", blockName, newBlockType, Boolean.TRUE.equals(response.getBody()) ? "Success" : "Failed");
        return Boolean.TRUE.equals(response.getBody());
    }
}
