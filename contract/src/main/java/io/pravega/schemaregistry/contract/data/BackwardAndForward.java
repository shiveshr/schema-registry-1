/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.contract.data;

import com.google.common.base.Preconditions;
import io.pravega.common.ObjectBuilder;
import lombok.Builder;
import lombok.Data;

/**
 * Defines different BackwardAndForward policy options for schema evolution for schemas within a group.
 * The choice of compatibility policy tells the Schema Registry service whether a schema should be accepted to evolve
 * into new schema by comparing it with one or more existing versions of the schema. 
 * 
 * {@link Backward}: a new schema can be used to read data written by previous schema. 
 * {@link BackwardTransitive}: a new schema can be used read data written by any of previous schemas. 
 * {@link BackwardTill}: a new schema can be used to read data written by any of previous schemas till schema 
 * identified by version {@link BackwardAndForward#backwardTill}. 
 * {@link Forward}: previous schema can be used to read data written by new schema. 
 * {@link ForwardTransitive}: all previous schemas can read data written by new schema. 
 * {@link ForwardTill}: All previous schemas till schema identified by version {@link BackwardAndForward#forwardTill}
 * can read data written by new schema. 
 * can read data written by new schema. New schema can be used to read data written by any of previous schemas till schema 
 * identified by version {@link BackwardAndForward#backwardTill}. 
 */
@Data
@Builder
public class BackwardAndForward implements Compatibility {

    private final BackwardPolicy backwardPolicy;
    private final ForwardPolicy forwardPolicy;

    public interface BackwardPolicy {
    }

    public interface ForwardPolicy {
    }

    public static class Backward implements BackwardPolicy {
    }

    @Data
    public static class BackwardTill implements BackwardPolicy {
        private final VersionInfo versionInfo;    
    }

    public static class BackwardTransitive implements BackwardPolicy {
    }

    public static class Forward implements ForwardPolicy {
    }

    @Data
    public static class ForwardTill implements ForwardPolicy {
        private final VersionInfo versionInfo;    
    }

    public static class ForwardTransitive implements ForwardPolicy {
    }

    private BackwardAndForward(BackwardPolicy backwardPolicy, ForwardPolicy forwardPolicy) {
        Preconditions.checkArgument(backwardPolicy != null || forwardPolicy != null);
        this.backwardPolicy = backwardPolicy;
        this.forwardPolicy = forwardPolicy;
    }
    
    /**
     * Method to create a compatibility policy of type backwardPolicy. BackwardPolicy policy implies new schema will be validated
     * to be capable of reading data written using the previous schema. 
     * 
     * @return BackwardAndForward with BackwardPolicy.
     */
    public static BackwardAndForward backward() {
        return new BackwardAndForward(new Backward(), null);
    }

    /**
     * Method to create a compatibility policy of type backwardPolicy till. BackwardTill policy implies new schema will be validated
     * to be capable of reading data written using the all previous schemas till version supplied as input.
     * 
     * @param backwardTill version till which schemas should be checked for compatibility.
     * @return BackwardAndForward with BackwardTill version.
     */
    public static BackwardAndForward backwardTill(VersionInfo backwardTill) {
        return new BackwardAndForward(new BackwardTill(backwardTill), null);
    }

    /**
     * Method to create a compatibility policy of type backwardPolicy transitive. BackwardPolicy transitive policy implies 
     * new schema will be validated to be capable of reading data written using the all previous schemas versions.
     * 
     * @return BackwardAndForward with BackwardTransitive.
     */
    public static BackwardAndForward backwardTransitive() {
        return new BackwardAndForward(new BackwardTransitive(), null);
    }

    /**
     * Method to create a compatibility policy of type forward. ForwardPolicy policy implies new schema will be validated
     * such that data written using new schema can be read using the previous schema. 
     *      
     * @return BackwardAndForward with ForwardPolicy
     */
    public static BackwardAndForward forward() {
        return new BackwardAndForward(null, new Forward());
    }

    /**
     * Method to create a compatibility policy of type forward till. ForwardPolicy policy implies new schema will be validated
     * such that data written using new schema can be read using the all previous schemas till supplied version. 
     *
     * @param forwardTill version till which schemas should be checked for compatibility.
     * @return BackwardAndForward with ForwardTill version.
     */
    public static BackwardAndForward forwardTill(VersionInfo forwardTill) {
        return new BackwardAndForward(null, new ForwardTill(forwardTill));
    }

    /**
     * Method to create a compatibility policy of type forward transitive. 
     * ForwardPolicy transitive policy implies new schema will be validated such that data written using new schema 
     * can be read using all previous schemas. 
     *      
     * @return BackwardAndForward with ForwardTransitive.
     */
    public static BackwardAndForward forwardTransitive() {
        return new BackwardAndForward(null, new ForwardTransitive());
    }

    /**
     * Method to create a compatibility policy of type full. Full means backwardPolicy and forward compatibility check with 
     * previous schema version. Which means new schema can be used to read data written with previous schema and vice versa. 
     * 
     * @return BackwardAndForward with Full.
     */
    public static BackwardAndForward full() {
        return new BackwardAndForward(new Backward(), new Forward());
    }

    /**
     * Method to create a compatibility policy of type full transitive. 
     * Full transitive means backwardPolicy and forward compatibility check with all previous schema version. 
     * This implies new schema can be used to read data written with any of the previous schemas and vice versa. 
     *
     * @return BackwardAndForward with FullTransitive.
     */
    public static BackwardAndForward fullTransitive() {
        return new BackwardAndForward(new BackwardTransitive(), new ForwardTransitive());
    }

    /**
     * Method to create a schemaValidationRules policy of type backwardPolicy till and forwardOne till. This is a combination of  
     * backwardPolicy till and forwardOne till policies. 
     * All previous schemas till schema identified by version {@link BackwardAndForward#forwardTill}
     * can read data written by new schema. New schema can be used to read data written by any of previous schemas till schema 
     * identified by version {@link BackwardAndForward#backwardTill}. 
     *
     * @param backwardTill version till which backwardPolicy schemaValidationRules is checked for.
     * @param forwardTill version till which forwardOne schemaValidationRules is checked for.
     * @return BackwardAndForward with backwardTillAndForwardTill.
     */
    public static BackwardAndForward backwardTillAndForwardTill(VersionInfo backwardTill, VersionInfo forwardTill) {
        return new BackwardAndForward(new BackwardTill(backwardTill), new ForwardTill(forwardTill));
    }

    /**
     * Method to create a schemaValidationRules policy of type backwardPolicy one and forwardOne till. 
     *
     * All previous schemas till schema identified by version {@link BackwardAndForward#forwardTill}
     * can read data written by new schema. New schema can be used to read data written by previous schema.
     *
     * @param forwardTill version till which forwardTill schemaValidationRules is checked for.
     * @return BackwardAndForward with backwardOneAndForwardTill.
     */
    public static BackwardAndForward backwardOneAndForwardTill(VersionInfo forwardTill) {
        return new BackwardAndForward(new Backward(), new ForwardTill(forwardTill));
    }

    /**
     * Method to create a schemaValidationRules policy of type backwardPolicy till one and forwardOne one. 
     *
     * All previous schemas till schema identified by version {@link BackwardAndForward#forwardTill}
     * can read data written by new schema. New schema can be used to read data written by previous schema.
     *
     * @param backwardTill version till which backwardTill schemaValidationRules is checked for.
     * @return BackwardAndForward with backwardTillAndForwardOne.
     */
    public static BackwardAndForward backwardTillAndForwardOne(VersionInfo backwardTill) {
        return new BackwardAndForward(new BackwardTill(backwardTill), new Forward());
    }
    
    public static class BackwardAndForwardBuilder implements ObjectBuilder<BackwardAndForward> {
    }
}
