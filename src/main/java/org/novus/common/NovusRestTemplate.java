package org.novus.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;

import org.novus.exception.NovusException;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This is customized Rest template to make rest calls using java and map it to respective domain object
 * 
 * @author Manjeet Duhan
 * @date Aug 19, 2018
 *
 * @param <T>
 */
public class NovusRestTemplate<T> {

  HttpURLConnection conn;

  public NovusRestTemplate(String vaultUrl) {
    try {
      URL url = new URL(vaultUrl);
      conn = (HttpURLConnection) url.openConnection();
      conn.setRequestProperty("Accept", "application/json");
      conn.setDoOutput(true);
    } catch (MalformedURLException e) {
      throw new RuntimeException(vaultUrl + " is not valid", e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void setRequestType(String methodType) {
    try {
      conn.setRequestMethod(methodType);
    } catch (ProtocolException e) {
      throw new RuntimeException(e);
    }
  }

  public HttpURLConnection getConnection() {
    return conn;
  }

  public void setHeader(String key, String value) {
    conn.setRequestProperty(key, value);
  }

  @SuppressWarnings("unchecked")
  public T getResponse(Class<T> type) {
    try {
      if (conn.getResponseCode() != 200) {
        throw new NovusException("Vault is sealed or VAULT_TOKEN is wrong for host " + conn.getURL().getHost());
      }
      BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));

      String output;
      StringBuilder data = new StringBuilder();
      while ((output = br.readLine()) != null) {
        data.append(output);
      }
      ObjectMapper mapper = new ObjectMapper();

      return mapper.readValue(data.toString(), type);

    } catch (IOException e) {
      throw new NovusException("Vault is down for host " + conn.getURL().getHost(), e);
    }

  }

}
