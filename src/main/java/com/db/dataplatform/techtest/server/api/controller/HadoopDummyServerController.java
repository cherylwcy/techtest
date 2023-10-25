package com.db.dataplatform.techtest.server.api.controller;

import com.db.dataplatform.techtest.server.component.Server;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.validation.Valid;
import java.util.Random;

/**
 * This code does not require any test coverage. Do not modify this class.
 */

@Slf4j
@Validated
@Controller
@RequestMapping("/hadoopserver")
@RequiredArgsConstructor
public class HadoopDummyServerController {

    private final Server server;

    @PostMapping(value = "/pushbigdata")
    public ResponseEntity<HttpStatus> pushBigData(@RequestBody @Valid String payload) throws InterruptedException {

        log.info("Saving to Hadoop file system:{}", payload);
        Random random = new Random();
        int workDuration = random.ints(2000, 4000).findAny().getAsInt();

        // Simulate long running work.
        Thread.sleep(workDuration);

        if(workDuration > 3000) {
            log.info("Hadoop back end has timed out");
            return ResponseEntity.status(HttpStatus.GATEWAY_TIMEOUT).build();
        }

        log.info("Saving to Hadoop file system - finished");
        return ResponseEntity.ok().build();
    }
}
