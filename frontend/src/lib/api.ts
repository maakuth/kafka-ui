import {
  KsqlApi,
  TopicsApi,
  SchemasApi,
  BrokersApi,
  MessagesApi,
  ClustersApi,
  Configuration,
  KafkaConnectApi,
  ConsumerGroupsApi,
  AuthorizationApi,
  ApplicationConfigApi,
  AclsApi,
  UnmappedApi,
} from 'generated-sources';
import { BASE_PARAMS } from 'lib/constants';

const redirectToLogin = () => {
  const loginPath = `${BASE_PARAMS.basePath}/login`;

  if (window.location.pathname !== loginPath) {
    window.location.replace(loginPath);
  }
};

const fetchWithSessionExpiryRedirect = async (
  input: RequestInfo | URL,
  init?: RequestInit
) => {
  const response = await fetch(input, init);

  if (response.status === 401) {
    redirectToLogin();
  }

  return response;
};

const apiClientConf = new Configuration({
  ...BASE_PARAMS,
  fetchApi: fetchWithSessionExpiryRedirect,
});

export const ksqlDbApiClient = new KsqlApi(apiClientConf);
export const topicsApiClient = new TopicsApi(apiClientConf);
export const brokersApiClient = new BrokersApi(apiClientConf);
export const schemasApiClient = new SchemasApi(apiClientConf);
export const messagesApiClient = new MessagesApi(apiClientConf);
export const clustersApiClient = new ClustersApi(apiClientConf);
export const kafkaConnectApiClient = new KafkaConnectApi(apiClientConf);
export const consumerGroupsApiClient = new ConsumerGroupsApi(apiClientConf);
export const authApiClient = new AuthorizationApi(apiClientConf);
export const appConfigApiClient = new ApplicationConfigApi(apiClientConf);
export const aclApiClient = new AclsApi(apiClientConf);
export const internalApiClient = new UnmappedApi(apiClientConf);
