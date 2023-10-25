package com.db.dataplatform.techtest.server.component;

import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.persistence.model.DataBodyEntity;
import org.springframework.http.HttpStatus;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface Server {
    boolean saveDataEnvelope(DataEnvelope envelope, String checksum) throws IOException, NoSuchAlgorithmException;
    List<DataEnvelope> getDataEnvelope(String blocktype);
    boolean updateDataBlockType(String name, String newBlockType);
    CompletableFuture<HttpStatus> saveDataLake(String payload);
}
