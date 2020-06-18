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
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import javax.validation.constraints.*;

/**
 * ForwardPolicy policy.
 */
@ApiModel(description = "ForwardPolicy policy.")

public class ForwardPolicy   {
  @JsonProperty("forwardPolicy")
  private Object forwardPolicy = null;

  public ForwardPolicy forwardPolicy(Object forwardPolicy) {
    this.forwardPolicy = forwardPolicy;
    return this;
  }

  /**
   * BackwardAndForward type forwardPolicy. Can be one of forward, forwardTill and forwardTransitive.
   * @return forwardPolicy
   **/
  @JsonProperty("forwardPolicy")
  @ApiModelProperty(required = true, value = "BackwardAndForward type forwardPolicy. Can be one of forward, forwardTill and forwardTransitive.")
  @NotNull
  public Object getForwardPolicy() {
    return forwardPolicy;
  }

  public void setForwardPolicy(Object forwardPolicy) {
    this.forwardPolicy = forwardPolicy;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ForwardPolicy forwardPolicy = (ForwardPolicy) o;
    return Objects.equals(this.forwardPolicy, forwardPolicy.forwardPolicy);
  }

  @Override
  public int hashCode() {
    return Objects.hash(forwardPolicy);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ForwardPolicy {\n");
    
    sb.append("    forwardPolicy: ").append(toIndentedString(forwardPolicy)).append("\n");
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

