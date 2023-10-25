package com.db.dataplatform.techtest.server.service.impl;

import com.db.dataplatform.techtest.server.persistence.model.DataHeaderEntity;
import com.db.dataplatform.techtest.server.persistence.repository.DataHeaderRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class DataHeaderServiceImpl implements com.db.dataplatform.techtest.server.service.DataHeaderService {

    private final DataHeaderRepository dataHeaderRepository;

    @Override
    public void saveHeader(DataHeaderEntity entity) {
        dataHeaderRepository.save(entity);
    }
}
