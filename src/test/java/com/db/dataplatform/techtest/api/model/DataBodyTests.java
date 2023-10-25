package com.db.dataplatform.techtest.api.model;

import com.db.dataplatform.techtest.server.api.model.DataBody;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class DataBodyTests {

    public static final String DUMMY_DATA = "AKCp5fU4WNWKBVvhXsbNhqk33tawri9iJUkA5o4A6YqpwvAoYjajVw8xdEw6r9796h1wEp29D";

    @Test
    public void assignDataBodyFieldsShouldWorkAsExpected() {
        DataBody dataBody = new DataBody(DUMMY_DATA);

        assertThat(dataBody).isNotNull();
        assertThat(dataBody.getDataBody()).isEqualTo(DUMMY_DATA);
    }
}
