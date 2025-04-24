/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.server;

import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.metadata.CatalogManager;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Inject;

import javax.annotation.security.RolesAllowed;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.server.security.RoleType.ADMIN;
import static com.facebook.presto.server.security.RoleType.USER;

@Path("/v1/catalog")
@RolesAllowed({ADMIN, USER})
public class CatalogResource
{
    private final CatalogManager catalogManager;

    @Inject
    public CatalogResource(CatalogManager catalogManager)
    {
        this.catalogManager = catalogManager;
    }

    @GET
    @Path("config")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getCatalogConfiguration()
    {
        List<CatalogConfiguration> configurations = new ArrayList<>();

        for (Catalog catalog : catalogManager.getCatalogs()) {
            configurations.add(new CatalogConfiguration(catalog.getCatalogName(), catalog.getConfigProperties()));
        }

        CatalogConfigurationList configurationList = new CatalogConfigurationList(configurations);

        return Response.ok(configurationList).build();
    }

    public static class CatalogConfigurationList
    {
        private final List<CatalogConfiguration> catalogs;

        public CatalogConfigurationList(List<CatalogConfiguration> catalogs)
        {
            this.catalogs = catalogs;
        }

        @JsonProperty
        public List<CatalogConfiguration> getCatalogs()
        {
            return catalogs;
        }
    }

    public static class CatalogConfiguration
    {
        private final String name;
        private final Map<String, String> properties;

        public CatalogConfiguration(
                String name,
                Map<String, String> properties)
        {
            this.name = name;
            this.properties = properties;
        }

        @JsonProperty
        public String getName()
        {
            return name;
        }

        @JsonProperty
        public Map<String, String> getProperties()
        {
            return properties;
        }
    }
}
