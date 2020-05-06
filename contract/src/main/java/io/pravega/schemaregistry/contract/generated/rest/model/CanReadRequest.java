/*
 * Pravega Schema Registry APIs
 * REST APIs for Pravega Schema Registry.
 *
 * OpenAPI spec version: 0.0.1
 * 
 *
 * NOTE: This class is auto generated by the swagger code generator program.
 * https://github.com/swagger-api/swagger-codegen.git
 * Do not edit the class manually.
 */


package io.pravega.schemaregistry.contract.generated.rest.model;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import javax.validation.constraints.*;

/**
 * CanReadRequest
 */

public class CanReadRequest   {
  @JsonProperty("schemaInfo")
  private SchemaInfo schemaInfo = null;

  public CanReadRequest schemaInfo(SchemaInfo schemaInfo) {
    this.schemaInfo = schemaInfo;
    return this;
  }

  /**
   * Get schemaInfo
   * @return schemaInfo
   **/
  @JsonProperty("schemaInfo")
  @ApiModelProperty(value = "")
  public SchemaInfo getSchemaInfo() {
    return schemaInfo;
  }

  public void setSchemaInfo(SchemaInfo schemaInfo) {
    this.schemaInfo = schemaInfo;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CanReadRequest canReadRequest = (CanReadRequest) o;
    return Objects.equals(this.schemaInfo, canReadRequest.schemaInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schemaInfo);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class CanReadRequest {\n");
    
    sb.append("    schemaInfo: ").append(toIndentedString(schemaInfo)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(java.lang.Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }
}

