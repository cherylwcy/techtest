package com.db.dataplatform.techtest.server.service;

import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import com.db.dataplatform.techtest.server.persistence.model.DataBodyEntity;

import java.util.List;

public interface DataBodyService {
    void saveDataBody(DataBodyEntity dataBody);
    List<DataBodyEntity> getDataBodyByBlockType(BlockTypeEnum blockType);
    List<DataBodyEntity> getDataBodyByBlockName(String blockName);
}
