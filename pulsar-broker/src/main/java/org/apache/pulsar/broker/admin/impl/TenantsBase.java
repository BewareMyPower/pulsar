/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.admin.impl;

import static org.apache.pulsar.common.naming.Constants.GLOBAL_CLUSTER;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.web.PulsarWebResource;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.naming.NamedEntity;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TenantOperation;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TenantsBase extends PulsarWebResource {

    private static final Logger log = LoggerFactory.getLogger(TenantsBase.class);

    @GET
    @ApiOperation(value = "Get the list of existing tenants.", response = String.class, responseContainer = "List")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 404, message = "Tenant doesn't exist")})
    public void getTenants(@Suspended final AsyncResponse asyncResponse) {
        final String clientAppId = clientAppId();
        validateBothSuperUserAndTenantOperation(null, TenantOperation.LIST_TENANTS)
                .thenCompose(__ -> tenantResources().listTenantsAsync())
                .thenAccept(tenants -> {
                    // deep copy the tenants to avoid concurrent sort exception
                    List<String> deepCopy = new ArrayList<>(tenants);
                    deepCopy.sort(null);
                    asyncResponse.resume(deepCopy);
                }).exceptionally(ex -> {
                    log.error("[{}] Failed to get tenants list", clientAppId, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}")
    @ApiOperation(value = "Get the admin configuration for a given tenant.", response = TenantInfo.class)
    @ApiResponses(value = {@ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 404, message = "Tenant does not exist")})
    public void getTenantAdmin(@Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "The tenant name") @PathParam("tenant") String tenant) {
        final String clientAppId = clientAppId();
        validateBothSuperUserAndTenantOperation(tenant, TenantOperation.GET_TENANT)
                .thenCompose(__ -> tenantResources().getTenantAsync(tenant))
                .thenApply(tenantInfo -> {
                    if (!tenantInfo.isPresent()) {
                        throw new RestException(Status.NOT_FOUND, "Tenant does not exist");
                    }
                    return tenantInfo.get();
                })
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    log.error("[{}] Failed to get tenant admin {}", clientAppId, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @PUT
    @Path("/{tenant}")
    @ApiOperation(value = "Create a new tenant.", notes = "This operation requires Pulsar super-user privileges.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 409, message = "Tenant already exists"),
            @ApiResponse(code = 412, message = "Tenant name is not valid"),
            @ApiResponse(code = 412, message = "Clusters can not be empty"),
            @ApiResponse(code = 412, message = "Clusters do not exist")})
    public void createTenant(@Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "The tenant name") @PathParam("tenant") String tenant,
            @ApiParam(value = "TenantInfo") TenantInfoImpl tenantInfo) {
        final String clientAppId = clientAppId();
        try {
            NamedEntity.checkName(tenant);
        } catch (IllegalArgumentException e) {
            log.warn("[{}] Failed to create tenant with invalid name {}", clientAppId, tenant, e);
            asyncResponse.resume(new RestException(Status.PRECONDITION_FAILED, "Tenant name is not valid"));
            return;
        }
        validateBothSuperUserAndTenantOperation(tenant, TenantOperation.CREATE_TENANT)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> validateClustersAsync(tenantInfo))
                .thenCompose(__ -> validateAdminRoleAsync(tenantInfo))
                .thenCompose(__ -> tenantResources().tenantExistsAsync(tenant))
                .thenAccept(exist -> {
                    if (exist) {
                        throw new RestException(Status.CONFLICT, "Tenant already exist");
                    }
                })
                .thenCompose(__ -> tenantResources().listTenantsAsync())
                .thenAccept(tenants -> {
                    int maxTenants = pulsar().getConfiguration().getMaxTenants();
                    // Due to the cost of distributed locks, no locks are added here.
                    // In a concurrent scenario, the threshold will be exceeded.
                    if (maxTenants > 0) {
                        if (tenants != null && tenants.size() >= maxTenants) {
                            throw new RestException(Status.PRECONDITION_FAILED, "Exceed the maximum number of tenants");
                        }
                    }
                })
                .thenCompose(__ -> tenantResources().createTenantAsync(tenant, tenantInfo))
                .thenAccept(__ -> {
                    log.info("[{}] Created tenant {}", clientAppId, tenant);
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(ex -> {
                    log.error("[{}] Failed to create tenant {}", clientAppId, tenant, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}")
    @ApiOperation(value = "Update the admins for a tenant.",
            notes = "This operation requires Pulsar super-user privileges.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 404, message = "Tenant does not exist"),
            @ApiResponse(code = 409, message = "Tenant already exists"),
            @ApiResponse(code = 412, message = "Clusters can not be empty"),
            @ApiResponse(code = 412, message = "Clusters do not exist")})
    public void updateTenant(@Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "The tenant name") @PathParam("tenant") String tenant,
            @ApiParam(value = "TenantInfo") TenantInfoImpl newTenantAdmin) {
        final String clientAppId = clientAppId();
        validateBothSuperUserAndTenantOperation(tenant, TenantOperation.UPDATE_TENANT)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> validateClustersAsync(newTenantAdmin))
                .thenCompose(__ -> validateAdminRoleAsync(newTenantAdmin))
                .thenCompose(__ -> tenantResources().getTenantAsync(tenant))
                .thenCompose(tenantAdmin -> {
                    if (!tenantAdmin.isPresent()) {
                        throw new RestException(Status.NOT_FOUND, "Tenant " + tenant + " not found");
                    }
                    TenantInfo oldTenantAdmin = tenantAdmin.get();
                    Set<String> newClusters = new HashSet<>(newTenantAdmin.getAllowedClusters());
                    return canUpdateCluster(tenant, oldTenantAdmin.getAllowedClusters(), newClusters);
                })
                .thenCompose(__ -> tenantResources().updateTenantAsync(tenant, old -> newTenantAdmin))
                .thenAccept(__ -> {
                    log.info("[{}] Successfully updated tenant info {}", clientAppId, tenant);
                    asyncResponse.resume(Response.noContent().build());
                }).exceptionally(ex -> {
                    log.warn("[{}] Failed to update tenant {}", clientAppId, tenant, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}")
    @ApiOperation(value = "Delete a tenant and all namespaces and topics under it.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Operation successful"),
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 404, message = "Tenant does not exist"),
            @ApiResponse(code = 405, message = "Broker doesn't allow forced deletion of tenants"),
            @ApiResponse(code = 409, message = "The tenant still has active namespaces")})
    public void deleteTenant(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") @ApiParam(value = "The tenant name") String tenant,
            @QueryParam("force") @DefaultValue("false") boolean force) {
        final String clientAppId = clientAppId();
        validateBothSuperUserAndTenantOperation(tenant, TenantOperation.DELETE_TENANT)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> internalDeleteTenant(tenant, force))
                .thenAccept(__ -> {
                    log.info("[{}] Deleted tenant {}", clientAppId, tenant);
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(ex -> {
                    Throwable cause = FutureUtil.unwrapCompletionException(ex);
                    log.error("[{}] Failed to delete tenant {}", clientAppId, tenant, cause);
                    if (cause instanceof IllegalStateException) {
                        asyncResponse.resume(new RestException(Status.CONFLICT, cause));
                    } else {
                        resumeAsyncResponseExceptionally(asyncResponse, cause);
                    }
                    return null;
                });
    }

    protected CompletableFuture<Void> internalDeleteTenant(String tenant, boolean force) {
        return force ? internalDeleteTenantAsyncForcefully(tenant) : internalDeleteTenantAsync(tenant);
    }

    protected CompletableFuture<Void> internalDeleteTenantAsync(String tenant) {
        return tenantResources().tenantExistsAsync(tenant)
                .thenAccept(exists -> {
                    if (!exists) {
                        throw new RestException(Status.NOT_FOUND, "Tenant doesn't exist");
                    }
                })
                .thenCompose(__ -> hasActiveNamespace(tenant))
                .thenCompose(__ -> tenantResources().deleteTenantAsync(tenant))
                .thenCompose(__ -> pulsar().getPulsarResources().getTopicResources().clearTenantPersistence(tenant))
                .thenCompose(__ -> pulsar().getPulsarResources().getNamespaceResources().deleteTenantAsync(tenant))
                .thenCompose(__ -> pulsar().getPulsarResources().getNamespaceResources()
                            .getPartitionedTopicResources().clearPartitionedTopicTenantAsync(tenant))
                .thenCompose(__ -> pulsar().getPulsarResources().getLocalPolicies()
                            .deleteLocalPoliciesTenantAsync(tenant))
                .thenCompose(__ -> pulsar().getPulsarResources().getLoadBalanceResources().getBundleDataResources()
                            .deleteBundleDataTenantAsync(tenant));
    }

    protected CompletableFuture<Void> internalDeleteTenantAsyncForcefully(String tenant) {
        if (!pulsar().getConfiguration().isForceDeleteTenantAllowed()) {
            return FutureUtil.failedFuture(
                    new RestException(Status.METHOD_NOT_ALLOWED, "Broker doesn't allow forced deletion of tenants"));
        }
        return tenantResources().getListOfNamespacesAsync(tenant)
                .thenApply(namespaces -> {
                    final List<CompletableFuture<Void>> futures = new ArrayList<>();
                    try {
                        PulsarAdmin adminClient = pulsar().getAdminClient();
                        for (String namespace : namespaces) {
                            futures.add(adminClient.namespaces().deleteNamespaceAsync(namespace, true));
                        }
                    } catch (Exception e) {
                        log.error("[{}] Failed to force delete namespaces {}", clientAppId(), namespaces, e);
                        throw new RestException(e);
                    }
                    return futures;
                })
                .thenCompose(futures -> FutureUtil.waitForAll(futures))
                .thenCompose(__ -> internalDeleteTenantAsync(tenant));
    }

    private CompletableFuture<Void> validateClustersAsync(TenantInfo info) {
        if (info == null) {
            return FutureUtil.failedFuture(new RestException(Status.PRECONDITION_FAILED, "TenantInfo is null"));
        }

        Set<String> allowedClusters = info.getAllowedClusters();
        if (allowedClusters == null) {
            return FutureUtil.failedFuture(new RestException(Status.PRECONDITION_FAILED, "Clusters cannot be null"));
        }

        Set<String> cleanedClusters = allowedClusters.stream()
                .filter(c -> !StringUtils.isBlank(c))
                .collect(Collectors.toSet());
        if (cleanedClusters.isEmpty() || allowedClusters.stream().anyMatch(StringUtils::isBlank)) {
            log.warn("[{}] Validation failed: allowed clusters are empty or contain blanks", clientAppId());
            return FutureUtil.failedFuture(
                    new RestException(Status.PRECONDITION_FAILED, "Clusters cannot be empty or blank"));
        }

        return clusterResources().listAsync().thenAccept(availableClusters -> {
            List<String> nonexistentClusters = allowedClusters.stream()
                    .filter(cluster -> !(availableClusters.contains(cluster) || GLOBAL_CLUSTER.equals(cluster)))
                    .collect(Collectors.toList());
            if (nonexistentClusters.size() > 0) {
                log.warn("[{}] Failed to validate due to clusters {} do not exist", clientAppId(), nonexistentClusters);
                throw new RestException(Status.PRECONDITION_FAILED, "Clusters do not exist");
            }
        });
    }

    private CompletableFuture<Void> validateAdminRoleAsync(TenantInfoImpl info) {
        if (info.getAdminRoles() != null && !info.getAdminRoles().isEmpty()) {
            for (String adminRole : info.getAdminRoles()) {
                if (!StringUtils.trim(adminRole).equals(adminRole)) {
                    log.warn("[{}] Failed to validate due to adminRole {} contains whitespace in the beginning or end.",
                            clientAppId(), adminRole);
                    return FutureUtil.failedFuture(new RestException(Status.PRECONDITION_FAILED,
                            "AdminRoles contains whitespace in the beginning or end."));
                }
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Boolean> validateBothSuperUserAndTenantOperation(String tenant,
                                                                               TenantOperation operation) {
        final var superUserValidationFuture = validateSuperUserAccessAsync();
        final var tenantOperationValidationFuture = validateTenantOperationAsync(tenant, operation);
        return CompletableFuture.allOf(superUserValidationFuture, tenantOperationValidationFuture)
                .handle((__, err) -> {
                    if (!superUserValidationFuture.isCompletedExceptionally()
                        || !tenantOperationValidationFuture.isCompletedExceptionally()) {
                        return true;
                    }
                    if (log.isDebugEnabled()) {
                        Throwable superUserValidationException = null;
                        try {
                            superUserValidationFuture.join();
                        } catch (Throwable ex) {
                            superUserValidationException = FutureUtil.unwrapCompletionException(ex);
                        }
                        Throwable brokerOperationValidationException = null;
                        try {
                            tenantOperationValidationFuture.join();
                        } catch (Throwable ex) {
                            brokerOperationValidationException = FutureUtil.unwrapCompletionException(ex);
                        }
                        log.debug("validateBothTenantOperationAndSuperUser failed."
                                  + " originalPrincipal={} clientAppId={} operation={} "
                                  + "superuserValidationError={} tenantOperationValidationError={}",
                                originalPrincipal(), clientAppId(), operation.toString(),
                                superUserValidationException, brokerOperationValidationException);
                    }
                    throw new RestException(Status.UNAUTHORIZED,
                            String.format("Unauthorized to validateBothTenantOperationAndSuperUser for"
                                          + " originalPrincipal [%s] and clientAppId [%s] "
                                          + "about operation [%s] ",
                                    originalPrincipal(), clientAppId(), operation.toString()));
                });
    }
}
