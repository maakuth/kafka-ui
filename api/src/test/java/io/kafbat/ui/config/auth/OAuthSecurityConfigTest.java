package io.kafbat.ui.config.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.security.authentication.AuthenticationCredentialsNotFoundException;

class OAuthSecurityConfigTest {

  private final OAuthSecurityConfig config = new OAuthSecurityConfig(mock(OAuthProperties.class));

  @Test
  void returnsUnauthorizedForUnauthenticatedApiRequest() {
    var exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/api/clusters").build());

    config.authenticationEntryPoint()
        .commence(exchange, new AuthenticationCredentialsNotFoundException("Session expired"))
        .block();

    assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
  }

  @Test
  void redirectsUnauthenticatedBrowserRequestToLogin() {
    var exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/").build());

    config.authenticationEntryPoint()
        .commence(exchange, new AuthenticationCredentialsNotFoundException("Session expired"))
        .block();

    assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.FOUND);
    assertThat(exchange.getResponse().getHeaders().getLocation()).hasPath("/login");
  }

}
