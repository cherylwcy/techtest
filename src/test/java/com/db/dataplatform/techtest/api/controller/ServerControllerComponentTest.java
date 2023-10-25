package com.db.dataplatform.techtest.api.controller;

import com.db.dataplatform.techtest.TestDataHelper;
import com.db.dataplatform.techtest.server.api.controller.ServerController;
import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.exception.HadoopClientException;
import com.db.dataplatform.techtest.server.component.Server;
import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.http.MediaType;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.web.util.UriTemplate;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;

@RunWith(MockitoJUnitRunner.class)
public class ServerControllerComponentTest {

	public static final String URI_PUSHDATA = "http://localhost:8090/dataserver/pushdata";
	public static final UriTemplate URI_GETDATA = new UriTemplate("http://localhost:8090/dataserver/data/{blockType}");
	public static final UriTemplate URI_PATCHDATA = new UriTemplate("http://localhost:8090/dataserver/update/{name}/{newBlockType}");

	@Mock
	private Server serverMock;

	private DataEnvelope testDataEnvelope;
	private ObjectMapper objectMapper;
	private MockMvc mockMvc;
	private ServerController serverController;

	@Before
	public void setUp() throws HadoopClientException, NoSuchAlgorithmException, IOException {
		serverController = new ServerController(serverMock);
		mockMvc = standaloneSetup(serverController).build();
		objectMapper = Jackson2ObjectMapperBuilder
				.json()
				.build();

		testDataEnvelope = TestDataHelper.createTestDataEnvelopeApiObject();

		when(serverMock.saveDataEnvelope(any(DataEnvelope.class), any(String.class))).thenReturn(true);

		List<DataEnvelope> mockDataEnvelopeList = new ArrayList<>();
		mockDataEnvelopeList.add(testDataEnvelope);
		when(serverMock.getDataEnvelope(BlockTypeEnum.BLOCKTYPEA.name())).thenReturn(mockDataEnvelopeList);

		when(serverMock.updateDataBlockType(testDataEnvelope.getDataHeader().getName(), BlockTypeEnum.BLOCKTYPEB.name())).thenReturn(true);
	}

	@Test
	public void testPushDataPostCallWorksAsExpected() throws Exception {

		String testDataEnvelopeJson = objectMapper.writeValueAsString(testDataEnvelope);

		MvcResult mvcResult = mockMvc.perform(post(URI_PUSHDATA)
						.header("Content-MD5", DigestUtils.md5Hex(testDataEnvelope.getDataBody().getDataBody()))
						.content(testDataEnvelopeJson)
						.contentType(MediaType.APPLICATION_JSON_VALUE))
				.andExpect(status().isOk())
				.andReturn();

		boolean checksumPass = Boolean.parseBoolean(mvcResult.getResponse().getContentAsString());
		assertThat(checksumPass).isTrue();
	}

	@Test
	public void testQueryData() throws Exception {

		Map<String, String> uriVariables = new HashMap<>();
		uriVariables.put("blockType", testDataEnvelope.getDataHeader().getBlockType().name());

		MvcResult mvcResult = mockMvc.perform(get(URI_GETDATA.expand(uriVariables)))
				.andExpect(status().isOk())
				.andReturn();

		String queryDataJson = mvcResult.getResponse().getContentAsString();

		List<DataEnvelope> mockDataEnvelopeList = new ArrayList<>();
		mockDataEnvelopeList.add(testDataEnvelope);
		String testDataEnvelopeJson = objectMapper.writeValueAsString(mockDataEnvelopeList);

		assertThat(queryDataJson).isEqualTo(testDataEnvelopeJson);
	}

	@Test
	public void testUpdateData() throws Exception {
		Map<String, String> uriVariables = new HashMap<>();
		uriVariables.put("name", testDataEnvelope.getDataHeader().getName());
		uriVariables.put("newBlockType", BlockTypeEnum.BLOCKTYPEB.name());

		MvcResult mvcResult = mockMvc.perform(get(URI_PATCHDATA.expand(uriVariables)))
				.andExpect(status().isOk())
				.andReturn();

		boolean success = Boolean.parseBoolean(mvcResult.getResponse().getContentAsString());
		assertThat(success).isTrue();
	}
}
