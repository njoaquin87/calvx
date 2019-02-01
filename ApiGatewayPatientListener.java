package com.qualifacts.carelogic.gateway.queue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.validation.Valid;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpMethod;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.util.ObjectUtils;
import org.springframework.web.util.UriComponentsBuilder;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.qualifacts.carelogic.common.exception.BusinessException;
import com.qualifacts.carelogic.common.response.BaseResponse;
import com.qualifacts.carelogic.configuration.model.Organization;
import com.qualifacts.carelogic.gateway.context.ApiGatewayContextUtil;
import com.qualifacts.carelogic.gateway.service.ConfigurationApiService;
import com.qualifacts.carelogic.gateway.service.ConsumerApiService;
import com.qualifacts.carelogic.gateway.service.util.Constants;

@Component
@ConditionalOnProperty("rabbitmq.enabled")
@RabbitListener(queues = "api.gateway.queue.patient", containerFactory = "listenerContainerFactory")
public class ApiGatewayPatientListener extends ApiGatewayQueueListener {

	private static final Logger LOGGER = LoggerFactory.getLogger(ApiGatewayPatientListener.class);

	@Autowired
	private ConsumerApiService consumerApiService;

	@Autowired
	private ConfigurationApiService configurationApiService;
	
	private static final String SECTION_CIH_INTEGRATION = "CIH Integration";
	private static final String SUB_SECTION_HIE_GENERAL_INT = "HIE General Integration";
	private static final String NAME_ID_NUMBER_TYPE = "ID Number Type";
	
	@RabbitHandler
	public void onMessage(@Valid @Payload Object message, @Headers Map<String, Object> headers) {
		super.onMessage(message, headers);
		//TODO: remove this section and related properties
		/*try {
			String patientId = (String) mapMessage.get(Constants.Patient.PATIENT_ID);
			
			if(StringUtils.isEmpty(patientId))
				throw new BusinessException("Parameter patientId is required.");
				
			consumerApiService.sendPatientToCIH(HYSTRIX_CMDKEY_CONSUMER_GET_PATIENT, URL_CONSUMER_GET_PATIENT_BY_ID, HYSTRIX_CMDKEY_CIH_SEND_NEW_PATIENT, URL_CIH_SEND_NEW_PATIENT,
					Long.parseLong(patientId));
		}
		catch (BusinessException e) {
			LOGGER.error(e.getMessage(), e);
		}*/
		
		try {
			LOGGER.info(" ----------ConsumerApiService------- Type:: " +  MapUtils.getString(mapMessage, Constants.Patient.TYPE));
			Map<String, Object> params = new HashMap<String, Object>();
			String patientId = MapUtils.getString(mapMessage, Constants.Patient.PATIENT_ID);
			Long documentId = MapUtils.getLong(mapMessage, Constants.Patient.DOCUMENT_ID);
			if (StringUtils.isEmpty(patientId) && documentId==null)
				throw new BusinessException("At least one of the parameters (patientId or documentId) is required.");

			params.put(Constants.Patient.PATIENT_ID, patientId);
			
			String type = MapUtils.getString(mapMessage, Constants.Patient.TYPE);
			String event = MapUtils.getString(mapMessage, Constants.Patient.EVENT);
			String payload = MapUtils.getString(mapMessage, Constants.Patient.PAYLOAD);
			Map<String, Object> payloadArray = getParametersPayload(payload);
			UriComponentsBuilder cihIntegrationUri = UriComponentsBuilder.fromUriString(URL_CIH_INTEGRATION)
					.queryParam(Constants.Patient.ENTITY, MapUtils.getString(mapMessage, Constants.Patient.ENTITY))
					.queryParam(Constants.Patient.OPERATION, MapUtils.getString(mapMessage, Constants.Patient.OPERATION))
					.queryParam(Constants.Patient.EVENT, event)
					.queryParam(Constants.Patient.TYPE, type)
					.queryParam(Constants.Patient.ORGANIZATION, MapUtils.getString(mapMessage, Constants.Patient.ORGANIZATION))
					.queryParam(Constants.Patient.CLIENT_PROGRAM_ID, MapUtils.getString(payloadArray, Constants.Patient.CLIENT_PROGRAM_ID))
					.queryParam(Constants.Patient.ACTIVITY_DETAIL_ID, MapUtils.getString(payloadArray, Constants.Patient.ACTIVITY_DETAIL_ID))
					.queryParam(Constants.Patient.ACTIVITY_LOG_ID, MapUtils.getString(payloadArray, Constants.Patient.ACTIVITY_LOG_ID))
					.queryParam(Constants.Patient.CLIENT_EPISODE_ID, MapUtils.getString(payloadArray, Constants.Patient.CLIENT_EPISODE_ID))
					.queryParam(Constants.Patient.PROGRAM_ID, MapUtils.getString(payloadArray, Constants.Patient.PROGRAM_ID))
					.queryParam(Constants.Patient.PROGRAM_NAME, MapUtils.getString(payloadArray, Constants.Patient.PROGRAM_NAME));


			String organizationId = MapUtils.getString(mapMessage, Constants.Patient.ORGANIZATION_ID);
            if(StringUtils.isNotEmpty(organizationId)) {
            	BaseResponse<String>  configValue = configurationApiService.getConfigurationValue(HYSTRIX_CMDKEY_CONFIGURATION_GET_VALUE_CONFIGURATIONS, SECTION_CIH_INTEGRATION,SUB_SECTION_HIE_GENERAL_INT,NAME_ID_NUMBER_TYPE, Long.valueOf(organizationId));
            	params.put(Constants.Patient.CONFIGURATION_VALUE, configValue.getResponse());
            }
            
			ApiGatewayContextUtil.getContext().getHeaders().put(Constants.Patient.REVOKE_CONSENT, MapUtils.getString(mapMessage, Constants.Patient.REVOKE_CONSENT));
			ApiGatewayContextUtil.getContext().getHeaders().put(Constants.Patient.REVOKE_SENSITIVE_CONSENT, MapUtils.getString(mapMessage, Constants.Patient.REVOKE_SENSITIVE_CONSENT));

			if (type!= null && Constants.Patient.TYPE_ADT.equals(type)) { 
				params.put(Constants.Patient.APPOINTMENT_STATUS, MapUtils.getString(mapMessage, Constants.Patient.APPOINTMENT_STATUS));
                UriComponentsBuilder uriBuilder = UriComponentsBuilder
                        .fromUriString(URL_CONSUMER_INTEGRATION_GET_PATIENT_BY_ID)
                        .queryParam(Constants.Patient.CLIENT_PROGRAM_ID,
                                MapUtils.getString(mapMessage, Constants.Patient.CLIENT_PROGRAM_ID))
                        .queryParam(Constants.Patient.APPOINTMENT_ID,
                                MapUtils.getString(mapMessage, Constants.Patient.APPOINTMENT_ID))
                        .queryParam(Constants.Patient.ORGANIZATION_ID,
                                MapUtils.getString(mapMessage, Constants.Patient.ORGANIZATION_ID))
                        .queryParam(Constants.Patient.EXTRACT_PERIOD,
                                MapUtils.getString(mapMessage, Constants.Patient.EXTRACT_PERIOD))
                        .queryParam(Constants.Patient.CLIENT_PROGRAM_BEGIN_DATE,
                                MapUtils.getString(mapMessage, Constants.Patient.CLIENT_PROGRAM_BEGIN_DATE))
                        .queryParam(Constants.Patient.DISCHARGE_DATE,
                                MapUtils.getString(mapMessage, Constants.Patient.DISCHARGE_DATE))
                        .queryParam(Constants.Patient.EVENT, MapUtils.getString(mapMessage, Constants.Patient.EVENT))
                        .queryParam(Constants.Patient.DIAGNOSIS_DOCUMENT_ID,
                                MapUtils.getString(mapMessage, Constants.Patient.DIAGNOSIS_DOCUMENT_ID))
                        .queryParam(Constants.Patient.STAFF_ID,
                                MapUtils.getString(mapMessage, Constants.Patient.STAFF_ID))
                        .queryParam(Constants.Patient.CLIENT_EPISODE_ID,
                                MapUtils.getString(payloadArray, Constants.Patient.CLIENT_EPISODE_ID))
                        .queryParam(Constants.Patient.ACTIVITY_ID,
                                MapUtils.getString(payloadArray, Constants.Patient.ACTIVITY_ID))
                        .queryParam(Constants.Patient.APPT_ORGANIZATION_ID,
                                MapUtils.getString(payloadArray, Constants.Patient.APPT_ORGANIZATION_ID)) 
                        .queryParam(Constants.Patient.DESCRIPTOR_NAME,
                                		(String)params.get(Constants.Patient.CONFIGURATION_VALUE))
                        .queryParam(Constants.Patient.DESCRIPTOR_TYPE,
                		        NAME_ID_NUMBER_TYPE)
                        .queryParam(Constants.Patient.MOD_CONSENT_CONFIG_ID, MapUtils.getLong(mapMessage, Constants.Patient.MOD_CONSENT_CONFIG_ID))
                        .queryParam(Constants.Patient.DISCHARGE_DISPOSITION_ID, MapUtils.getLong(payloadArray, Constants.Patient.DISCHARGE_DISPOSITION_ID))
                        .queryParam(Constants.Patient.DISCHARGE_LOCATION_ID, MapUtils.getLong(payloadArray, Constants.Patient.DISCHARGE_LOCATION_ID));
				
				//TODO: change hystrix cmdkey cih to a generic one and not by domain (patient)
				LOGGER.info("before sendPatientToCIH - params:: " + params);
				consumerApiService.sendPatientToCIH(
						HYSTRIX_CMDKEY_CONSUMER_GET_PATIENT,
						uriBuilder.build().toString(),
						HYSTRIX_CMDKEY_CIH_SEND_NEW_PATIENT,
						cihIntegrationUri.build().toString(),
						params);
			}
			
			if (type!= null && Constants.Patient.TYPE_CCDA.equals(type)) {
				
				params.put(Constants.Patient.EXTRACT_PERIOD, MapUtils.getString(mapMessage, Constants.Patient.EXTRACT_PERIOD));
				params.put(Constants.Patient.ORGANIZATION_ID, MapUtils.getString(mapMessage, Constants.Patient.ORGANIZATION_ID));
				params.put(Constants.Patient.EXCLUSIONS, MapUtils.getString(mapMessage, Constants.Patient.EXCLUSIONS));
				params.put(Constants.Patient.ORGANIZATION, MapUtils.getString(mapMessage, Constants.Patient.ORGANIZATION));
				params.put(Constants.Patient.EVENT, event);
				
				LOGGER.debug("before sendCCDAToCIH - params:: " + params);

                UriComponentsBuilder uriBuilder = UriComponentsBuilder
                        .fromUriString(URL_CONSUMER_INTEGRATION_GET_PATIENT_BY_ID)
                        .queryParam(Constants.Patient.CLIENT_PROGRAM_ID,
                                MapUtils.getString(mapMessage, Constants.Patient.CLIENT_PROGRAM_ID))
                        .queryParam(Constants.Patient.APPOINTMENT_ID,
                                MapUtils.getString(mapMessage, Constants.Patient.APPOINTMENT_ID))
                        .queryParam(Constants.Patient.ORGANIZATION_ID,
                                MapUtils.getString(mapMessage, Constants.Patient.ORGANIZATION_ID))
                        .queryParam(Constants.Patient.EXTRACT_PERIOD,
                                MapUtils.getString(mapMessage, Constants.Patient.EXTRACT_PERIOD))
                        .queryParam(Constants.Patient.CLIENT_PROGRAM_BEGIN_DATE,
                                MapUtils.getString(mapMessage, Constants.Patient.CLIENT_PROGRAM_BEGIN_DATE))
                        .queryParam(Constants.Patient.DISCHARGE_DATE,
                                MapUtils.getString(mapMessage, Constants.Patient.DISCHARGE_DATE))
                        .queryParam(Constants.Patient.STAFF_ID,
                                MapUtils.getString(mapMessage, Constants.Patient.STAFF_ID))
                        .queryParam(Constants.Patient.CLIENT_EPISODE_ID,
                                MapUtils.getString(payloadArray, Constants.Patient.CLIENT_EPISODE_ID))
                        .queryParam(Constants.Patient.ACTIVITY_ID,
                                MapUtils.getString(payloadArray, Constants.Patient.ACTIVITY_ID))
                        .queryParam(Constants.Patient.APPT_ORGANIZATION_ID,
                                MapUtils.getString(payloadArray, Constants.Patient.APPT_ORGANIZATION_ID))
                        .queryParam(Constants.Patient.MOD_CONSENT_CONFIG_ID, MapUtils.getLong(mapMessage, Constants.Patient.MOD_CONSENT_CONFIG_ID))
                        .queryParam(Constants.Patient.DISCHARGE_DISPOSITION_ID, MapUtils.getLong(payloadArray, Constants.Patient.DISCHARGE_DISPOSITION_ID))
                        .queryParam(Constants.Patient.DISCHARGE_LOCATION_ID, MapUtils.getLong(payloadArray, Constants.Patient.DISCHARGE_LOCATION_ID));

				consumerApiService.sendCCDAToCIH(
						HYSTRIX_CMDKEY_CONSUMER_GET_PATIENT,
						uriBuilder.build().toString(),
						HYSTRIX_CMDKEY_CIH_SEND_NEW_PATIENT,
						cihIntegrationUri.build().toString(),
						params);
				
			}
			
			if (event!=null && Constants.Patient.EVENT_REFER_PATIENT.equals(event)) {
				
				cihIntegrationUri = UriComponentsBuilder.fromUriString(URL_CIH_SEND_PATIENT_TO_MYSTRENGTH)
						.queryParam(Constants.Patient.ENTITY, MapUtils.getString(mapMessage, Constants.Patient.ENTITY))
						.queryParam(Constants.Patient.OPERATION, MapUtils.getString(mapMessage, Constants.Patient.OPERATION))
						.queryParam(Constants.Patient.EVENT, event)
						.queryParam(Constants.Patient.TYPE, type)
						.queryParam(Constants.Patient.ORGANIZATION, MapUtils.getString(mapMessage, Constants.Patient.ORGANIZATION));
				
				params.put(Constants.Patient.CLINICIAN_ID, MapUtils.getString(mapMessage, Constants.Patient.CLINICIAN_ID));
				params.put(Constants.Patient.ORGANIZATION_ACCESS_CODE, MapUtils.getString(mapMessage, Constants.Patient.ORGANIZATION_ACCESS_CODE));
				params.put(Constants.Patient.ORGANIZATION_LOGIN, MapUtils.getString(mapMessage, Constants.Patient.ORGANIZATION_LOGIN));
				params.put(Constants.Patient.ORGANIZATION_PASSWORD, MapUtils.getString(mapMessage, Constants.Patient.ORGANIZATION_PASSWORD));
				params.put(Constants.Patient.ORGANIZATION_SECRET_CODE, MapUtils.getString(mapMessage, Constants.Patient.ORGANIZATION_SECRET_CODE));
				consumerApiService.sendPersonContactInfoToCIH(
						HYSTRIX_CMDKEY_CIH_SEND_NEW_PATIENT,
						HYSTRIX_CMDKEY_CIH_SEND_CONTACT_INFO_PATIENT,
						URL_CONSUMER_INTEGRATION_GET_PATIENT_CONTACT_INFO_BY_ID,
						cihIntegrationUri.build().toString(),
						params);
			}
			
			if (type != null && Constants.Patient.TYPE_GNRINT.equals(type)) {
				sendMessageToCih(event, type, mapMessage, params);
			}
			
			
		} catch (BusinessException e) {
			LOGGER.error(e.getMessage(), e);
		} catch (JsonProcessingException e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	private Map<String, Object> getParametersPayload(String payload) {
	
		ObjectMapper jsonObjectMapper = new ObjectMapper();
		Map<String, Object> payloadArray = null;
		try {
			if (payload !=null && !payload.equals("")  && !payload.isEmpty()) {
			   payloadArray=jsonObjectMapper.readValue(payload, Map.class);
			}
		} catch (JsonParseException e) {
			LOGGER.error(e.getMessage(), e);
		} catch (JsonMappingException e) {
			LOGGER.error(e.getMessage(), e);
		} catch (IOException e) {
			LOGGER.error(e.getMessage(), e);
		}
		return payloadArray;
	}

	private void sendMessageToCih(String event, String type, Map<String, Object> payload, Map<String, Object> params)
			throws BusinessException, JsonProcessingException {
		Map<String, Object> payloadArray = getParametersPayload(MapUtils.getString(mapMessage, Constants.Patient.PAYLOAD));
		UriComponentsBuilder cihIntegrationUri = UriComponentsBuilder.fromUriString(URL_CIH_INTEGRATION)
				.queryParam(Constants.Patient.ENTITY, MapUtils.getString(payload, Constants.Patient.ENTITY))
				.queryParam(Constants.Patient.OPERATION, MapUtils.getString(payload, Constants.Patient.OPERATION))
				.queryParam(Constants.Patient.EVENT, event)
				.queryParam(Constants.Patient.TYPE, type)
				.queryParam(Constants.Patient.ORGANIZATION, MapUtils.getString(payload, Constants.Patient.ORGANIZATION))
				.queryParam(Constants.Patient.CLIENT_PROGRAM_ID, MapUtils.getString(payloadArray, Constants.Patient.CLIENT_PROGRAM_ID))
				.queryParam(Constants.Patient.ACTIVITY_DETAIL_ID, MapUtils.getString(payloadArray, Constants.Patient.ACTIVITY_DETAIL_ID))
				.queryParam(Constants.Patient.ACTIVITY_LOG_ID, MapUtils.getString(payloadArray, Constants.Patient.ACTIVITY_LOG_ID))
				.queryParam(Constants.Patient.CLIENT_EPISODE_ID, MapUtils.getString(payloadArray, Constants.Patient.CLIENT_EPISODE_ID))
				.queryParam(Constants.Patient.PROGRAM_ID, MapUtils.getString(payloadArray, Constants.Patient.PROGRAM_ID))
				.queryParam(Constants.Patient.PROGRAM_NAME, MapUtils.getString(payloadArray, Constants.Patient.PROGRAM_NAME));

		Map<String, Object> cihRequest = preparedRequest(event, type, payload, params);
		cihIntegrationUri.queryParam(Constants.Patient.MEASURE_NAME, MapUtils.getString(cihRequest, Constants.Patient.MEASURE_NAME));
		
		consumerApiService.sendMessageToCIH(HYSTRIX_CMDKEY_CIH_SEND_NEW_PATIENT, cihIntegrationUri.build().toString(),
				cihRequest);

	}

    private Map<String, Object> preparedRequest(String event, String type, Map<String, Object> payload,
            Map<String, Object> params) {

        if (StringUtils.isNotEmpty(event) && !ObjectUtils.isEmpty(payload)) {

            Map<String, Object> patientInfo = getClientInformation(event, payload, params);
            params.putAll(patientInfo);
            params.put(Constants.Patient.ORGANIZATION_ID,
                    MapUtils.getString(payload, Constants.Patient.ORGANIZATION_ID));
            params.put(Constants.Patient.ORGANIZATION, MapUtils.getString(payload, Constants.Patient.ORGANIZATION));
            params.put(Constants.Patient.APPOINTMENT_STATUS,
                    MapUtils.getString(mapMessage, Constants.Patient.APPOINTMENT_STATUS));

            Map<String, Object> payloadArray = getParametersPayload(
                    MapUtils.getString(payload, Constants.Patient.PAYLOAD));
            params.put(Constants.Patient.APPT_ORGANIZATION_ID,
                    MapUtils.getString(payloadArray, Constants.Patient.APPT_ORGANIZATION_ID));

            Long organizationId = MapUtils.getLong(payload, Constants.Patient.ORGANIZATION_ID);
            if (organizationId != null) {
                List<String> genealogyOrganizationList = getGenealogyOrganization(organizationId);
                params.put(Constants.Patient.GENEALOGY_ORGANIZATION, genealogyOrganizationList);
            }

            params = getExclusions(params, payload);

        }

        return params;
    }
	
	private List<String> getGenealogyOrganization(Long organizationId) {

		Map<String,Object> params = new HashMap<String,Object>();
		params.put(Constants.Param.ORGANIZATION_ID, organizationId);
		
		List<String> genealogyOrganizationList = new ArrayList<>();
		Organization organization = configurationApiService.exchangeForResponseEntity(
				HYSTRIX_CMDKEY_CONFIGURATION_GET_ORGANIZATION, URL_CONFIGURATION_GET_ORGANIZATION_GENEALOGY,
				Organization.class, null, HttpMethod.GET, params);
		
		if (!ObjectUtils.isEmpty(organization)) {
			genealogyOrganizationList.add(String.valueOf(organization.getId()));
			Organization organizationParent = organization.getParent();
			while (organizationParent != null) {
				genealogyOrganizationList.add(String.valueOf(organizationParent.getId()));
				organizationParent = organizationParent.getParent();
			}
		}
		
		return genealogyOrganizationList;
	}

	private Map<String, Object> getClientInformation(String event, Map<String, Object> payload, Map<String, Object> params) {
		if (Constants.Patient.EVENT_SIGN_CANS.equals(event)) {
			UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromUriString(URL_CONSUMER_INTEGRATION_GET_PATIENT_CANS)
					.queryParam(Constants.Patient.DOCUMENT_ID, MapUtils.getString(payload, Constants.Patient.DOCUMENT_ID));
			Map<String, Object> patientInfo = consumerApiService.getPatientInfo(HYSTRIX_CMDKEY_CONSUMER_GET_PATIENT,
					uriBuilder.build().toString(), params);
			return patientInfo;
		} else {
		    Map<String, Object> payloadArray = getParametersPayload(
		            MapUtils.getString(payload, Constants.Patient.PAYLOAD));
            String appointmentId = MapUtils.getString(payload, Constants.Patient.APPOINTMENT_ID);

            if (StringUtils.isEmpty(appointmentId)) {
                appointmentId = MapUtils.getString(payloadArray, Constants.Patient.ACTIVITY_LOG_ID);
            }
            
            UriComponentsBuilder uriBuilder = UriComponentsBuilder
                    .fromUriString(URL_CONSUMER_INTEGRATION_GET_PATIENT_BY_ID)
                    .queryParam(Constants.Patient.CLIENT_PROGRAM_ID,
                            MapUtils.getString(payload, Constants.Patient.CLIENT_PROGRAM_ID))
                    .queryParam(Constants.Patient.APPOINTMENT_ID, appointmentId)
                    .queryParam(Constants.Patient.ORGANIZATION_ID,
                            MapUtils.getString(payload, Constants.Patient.ORGANIZATION_ID))
                    .queryParam(Constants.Patient.EXTRACT_PERIOD,
                            MapUtils.getString(payload, Constants.Patient.EXTRACT_PERIOD))
                    .queryParam(Constants.Patient.CLIENT_PROGRAM_BEGIN_DATE,
                            MapUtils.getString(payload, Constants.Patient.CLIENT_PROGRAM_BEGIN_DATE))
                    .queryParam(Constants.Patient.DISCHARGE_DATE,
                            MapUtils.getString(payload, Constants.Patient.DISCHARGE_DATE))
                    .queryParam(Constants.Patient.STAFF_ID, MapUtils.getString(payload, Constants.Patient.STAFF_ID))
                    .queryParam(Constants.Patient.CLIENT_EPISODE_ID,
                            MapUtils.getLong(payloadArray, Constants.Patient.CLIENT_EPISODE_ID))
                    .queryParam(Constants.Patient.ACTIVITY_ID,
                            MapUtils.getLong(payloadArray, Constants.Patient.ACTIVITY_ID))
                    .queryParam(Constants.Patient.APPT_ORGANIZATION_ID,
                            MapUtils.getLong(payloadArray, Constants.Patient.APPT_ORGANIZATION_ID))
                    .queryParam(Constants.Patient.DOCUMENT_ID,
                            MapUtils.getString(payload, Constants.Patient.DOCUMENT_ID))
                    .queryParam(Constants.Patient.DESCRIPTOR_NAME,
                    		(String)params.get(Constants.Patient.CONFIGURATION_VALUE))
                    .queryParam(Constants.Patient.DESCRIPTOR_TYPE,
            		        NAME_ID_NUMBER_TYPE)
                    .queryParam(Constants.Patient.MOD_CONSENT_CONFIG_ID, MapUtils.getString(payload, Constants.Patient.MOD_CONSENT_CONFIG_ID))
                    .queryParam(Constants.Patient.DISCHARGE_DISPOSITION_ID, MapUtils.getLong(payloadArray, Constants.Patient.DISCHARGE_DISPOSITION_ID))
                    .queryParam(Constants.Patient.DISCHARGE_LOCATION_ID, MapUtils.getLong(payloadArray, Constants.Patient.DISCHARGE_LOCATION_ID));
            

			Map<String, Object> patientInfo = consumerApiService.getPatientInfo(HYSTRIX_CMDKEY_CONSUMER_GET_PATIENT,
					uriBuilder.build().toString(), params);
			return patientInfo;
		}
	}
	
	@SuppressWarnings("unchecked")
	public Map<String, Object> getExclusions(Map<String, Object> params, Map<String, Object> payload) {
		
		params.put(Constants.Patient.REQUEST_DATE, LocalDate.now().toString());
		
		Integer extrationPeriod = MapUtils.getInteger(payload, Constants.Patient.EXTRACT_PERIOD_GEN);
		if (extrationPeriod!=null) {
			params.put(Constants.Patient.CCDA_BEGIN_DATE, LocalDate.now().minusMonths(extrationPeriod).toString());
			params.put(Constants.Patient.CCDA_END_DATE, LocalDate.now().toString());
		}
		
		String exclusions = MapUtils.getString(payload, Constants.Patient.EXCLUSIONS);
		if (StringUtils.isNotEmpty(exclusions)) {
			LOGGER.debug("::: GetExclusions exclusions = {} :::", exclusions);
		
			ObjectMapper jsonObjectMapper = new ObjectMapper();
			Map<String, Object> exclusionsArray = null;
			
			try {
				exclusionsArray = jsonObjectMapper.readValue(exclusions, Map.class);
			} catch (IOException e) {
				LOGGER.error(e.getMessage(), e);
			}
			
			if (!ObjectUtils.isEmpty(exclusionsArray)) {
				params.put(Constants.Patient.HAS_EXCLUSIONS, true);
				params.put(Constants.Patient.EXCLUSIONS, exclusionsArray.get(Constants.Patient.EXCLUSIONS_ARRAY));
			}else {
				params.put(Constants.Patient.HAS_EXCLUSIONS, false);
			}
		}
		
		return params;
	}
}
