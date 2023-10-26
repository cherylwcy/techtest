package com.db.dataplatform.techtest.server.component.impl;

import com.db.dataplatform.techtest.server.api.model.DataBody;
import com.db.dataplatform.techtest.server.api.model.DataHeader;
import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import com.db.dataplatform.techtest.server.persistence.model.DataBodyEntity;
import com.db.dataplatform.techtest.server.persistence.model.DataHeaderEntity;
import com.db.dataplatform.techtest.server.service.DataBodyService;
import com.db.dataplatform.techtest.server.component.Server;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.modelmapper.ModelMapper;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class ServerImpl implements Server {

    private final DataBodyService dataBodyServiceImpl;
    private final ModelMapper modelMapper;

    public static final String URI_DATALAKE_PUSHDATA = "http://localhost:8090/hadoopserver/pushbigdata";

    /**
     * Save the data block in DB if checksum equals to the calculated checksum
     * @param envelope Data block
     * @param checksum MD5 checksum from HTTP header "Content-MD5"
     * @return true if there is a match with the client provided checksum.
     */
    @Override
    public boolean saveDataEnvelope(DataEnvelope envelope, String checksum) {

        // Check
        String calculatedCheckSum = DigestUtils.md5Hex(envelope.getDataBody().getDataBody());
        if (checksum != null && !checksum.equals(calculatedCheckSum))
            return false;

        // Save to persistence.
        persist(envelope);
        log.info("Data persisted successfully, data name: {}", envelope.getDataHeader().getName());
        return true;
    }

    /**
     * Save the DataEnvelope into DB
     * @param envelope DataEnvelop
     */
    private void persist(DataEnvelope envelope) {
        log.info("Persisting data with attribute name: {}", envelope.getDataHeader().getName());
        DataHeaderEntity dataHeaderEntity = modelMapper.map(envelope.getDataHeader(), DataHeaderEntity.class);

        DataBodyEntity dataBodyEntity = modelMapper.map(envelope.getDataBody(), DataBodyEntity.class);
        dataBodyEntity.setDataHeaderEntity(dataHeaderEntity);

        saveData(dataBodyEntity);
    }

    /**
     * Save DataBodyEntity into DB
     * @param dataBodyEntity Data model of DataEnvelop
     */
    private void saveData(DataBodyEntity dataBodyEntity) {
        dataBodyServiceImpl.saveDataBody(dataBodyEntity);
    }

    /**
     * Get a list of DataEnvelop with given blocktype
     * @param blocktype Name of BlockTypeEnum
     * @return List of DataEnvelope
     */
    public List<DataEnvelope> getDataEnvelope(String blocktype) {
        log.info("Get with blocktype: {}" , blocktype);

        for(BlockTypeEnum c : BlockTypeEnum.values()) {
            if (c.name().equals(blocktype)) {
                return getDataEnvelopes(dataBodyServiceImpl.getDataBodyByBlockType(c));
            }
        }
        return null;
    }

    /**
     * Convert a list of DataBodyEntity to DataEnvelop
     * @param dataBodyEntityList List of DataBodyEntity
     * @return List of DataEnvelop
     */
    private static List<DataEnvelope> getDataEnvelopes(List<DataBodyEntity> dataBodyEntityList) {
        List<DataEnvelope> dataEnvelopeList = new ArrayList<>();
        for (DataBodyEntity dataBodyEntity : dataBodyEntityList) {
            DataBody dataBody = new DataBody(dataBodyEntity.getDataBody());
            DataHeader dataHeader = new DataHeader(dataBodyEntity.getDataHeaderEntity().getName(), dataBodyEntity.getDataHeaderEntity().getBlocktype());
            DataEnvelope dataEnvelope = new DataEnvelope(dataHeader, dataBody);
            dataEnvelopeList.add(dataEnvelope);
        }
        return dataEnvelopeList;
    }

    /**
     * Update data block with block name in DB with the new block type
     * @param name Block name
     * @param newBlockType New block type
     * @return True if update success, else false
     */
    public boolean updateDataBlockType(String name, String newBlockType) {
        log.info("Get name & newBlockType: {}, {}", name, newBlockType);

        List<DataBodyEntity> dataBodyEntityList = dataBodyServiceImpl.getDataBodyByBlockName(name);
        if (CollectionUtils.isEmpty(dataBodyEntityList))
            return false;

        DataBodyEntity dataBodyEntity = dataBodyEntityList.get(0);
        dataBodyEntity.getDataHeaderEntity().setBlocktype(BlockTypeEnum.valueOf(newBlockType));
        dataBodyServiceImpl.saveDataBody(dataBodyEntity);
        return true;
    }

    /**
     * Async method for pushing payload to Hadoop data lake
     * @param payload Payload string
     * @return Http Status 200 OK if success, else Http 504 timeout
     */
    @Async
    public CompletableFuture<HttpStatus> saveDataLake(String payload) {
        log.info("Save payload {} to data lake: {}", payload, URI_DATALAKE_PUSHDATA);

        RestTemplate restTemplate = new RestTemplate();
        ResponseEntity<HttpStatus> response = restTemplate.postForEntity(URI_DATALAKE_PUSHDATA, payload, HttpStatus.class);

        return CompletableFuture.completedFuture(response.getStatusCode());
    }

}
