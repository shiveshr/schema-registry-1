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
import io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import javax.validation.constraints.*;

/**
 * ForwardPolicy compatibility which tells the service to check for forwardPolicy compatibility with all previous schemas till specific version.
 */
@ApiModel(description = "ForwardPolicy compatibility which tells the service to check for forwardPolicy compatibility with all previous schemas till specific version.")

public class ForwardTill   {
  @JsonProperty("name")
  private String name = null;

  @JsonProperty("version")
  private VersionInfo version = null;

  public ForwardTill name(String name) {
    this.name = name;
    return this;
  }

  /**
   * Get name
   * @return name
   **/
  @JsonProperty("name")
  @ApiModelProperty(required = true, value = "")
  @NotNull
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public ForwardTill version(VersionInfo version) {
    this.version = version;
    return this;
  }

  /**
   * Whether given schema is valid with respect to existing group schemas against the configured compatibility.
   * @return version
   **/
  @JsonProperty("version")
  @ApiModelProperty(required = true, value = "Whether given schema is valid with respect to existing group schemas against the configured compatibility.")
  @NotNull
  public VersionInfo getVersion() {
    return version;
  }

  public void setVersion(VersionInfo version) {
    this.version = version;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ForwardTill forwardTill = (ForwardTill) o;
    return Objects.equals(this.name, forwardTill.name) &&
        Objects.equals(this.version, forwardTill.version);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, version);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ForwardTill {\n");
    
    sb.append("    name: ").append(toIndentedString(name)).append("\n");
    sb.append("    version: ").append(toIndentedString(version)).append("\n");
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

