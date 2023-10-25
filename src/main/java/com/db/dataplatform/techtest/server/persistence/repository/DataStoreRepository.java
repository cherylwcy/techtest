package com.db.dataplatform.techtest.server.persistence.repository;

import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import com.db.dataplatform.techtest.server.persistence.model.DataBodyEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface DataStoreRepository extends JpaRepository<DataBodyEntity, Long> {
    List<DataBodyEntity> findByDataHeaderEntity_Blocktype(BlockTypeEnum blocktype);
    List<DataBodyEntity> findByDataHeaderEntity_Name(String name);
}
