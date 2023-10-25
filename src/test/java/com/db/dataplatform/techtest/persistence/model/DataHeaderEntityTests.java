package com.db.dataplatform.techtest.persistence.model;

import com.db.dataplatform.techtest.TestDataHelper;
import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import com.db.dataplatform.techtest.server.persistence.model.DataHeaderEntity;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Instant;

import static com.db.dataplatform.techtest.TestDataHelper.TEST_NAME;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class DataHeaderEntityTests {


    @Test
    public void assignDataHeaderEntityFieldsShouldWorkAsExpected() {
        Instant expectedTimestamp = Instant.now();

        DataHeaderEntity dataHeaderEntity = TestDataHelper.createTestDataHeaderEntity(expectedTimestamp);

        assertThat(dataHeaderEntity.getName()).isEqualTo(TEST_NAME);
        assertThat(dataHeaderEntity.getBlocktype()).isEqualTo(BlockTypeEnum.BLOCKTYPEA);
        assertThat(dataHeaderEntity.getCreatedTimestamp()).isEqualTo(expectedTimestamp);
    }


}
