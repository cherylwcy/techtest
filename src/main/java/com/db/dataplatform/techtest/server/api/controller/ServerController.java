package com.db.dataplatform.techtest.server.api.controller;

import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.component.Server;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.HttpServerErrorException;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Controller
@RequestMapping("/dataserver")
@RequiredArgsConstructor
@Validated
public class ServerController {

    private final Server server;

    @PostMapping(value = "/pushdata", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Boolean> pushData(@Valid @RequestBody DataEnvelope dataEnvelope, @RequestHeader("Content-MD5") String md5) throws IOException, NoSuchAlgorithmException {
        log.info("Data envelope received: {}", dataEnvelope.getDataHeader().getName());
        boolean checksumPass = server.saveDataEnvelope(dataEnvelope, md5);
        log.info("Data envelope persisted. Attribute name: {}", dataEnvelope.getDataHeader().getName());

        if (checksumPass) {
            CompletableFuture<HttpStatus> completableFuture = server.saveDataLake(dataEnvelope.getDataBody().getDataBody());
            completableFuture.thenAccept(httpStatus -> log.info("DataLake finished with HTTP status Code: {}", httpStatus))
                    .exceptionally(ex -> {
                        if (ex.getCause() instanceof HttpServerErrorException) {
                            log.error("DataLake exception with Http status Code: {}", ((HttpServerErrorException)ex.getCause()).getStatusCode());
                        }
                        else {
                            log.error("DataLake exception: {}", ex.getMessage());
                        }
                        return null;
                    });

        }
        return ResponseEntity.ok(checksumPass);
    }

    @GetMapping(value = "/data/{blockType}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<List<DataEnvelope>> queryData(@PathVariable String blockType) {
        log.info("Querying data with blockType="+blockType);
        List<DataEnvelope> result = server.getDataEnvelope(blockType);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @GetMapping(value = "/update/{name}/{newBlockType}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Boolean> updateData(@PathVariable @NotNull @Size(max=30) String name, @PathVariable @NotNull @Size(max=11) String newBlockType) {
        log.info("Updating {} to new block type {}", name, newBlockType);
        boolean result = server.updateDataBlockType(name, newBlockType);
        return ResponseEntity.ok(result);
    }
}
