/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.opensearch.common.settings.Settings;
import org.opensearch.datafusion.core.SessionContext;
import org.opensearch.env.Environment;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for DataFusionService
 *
 * Note: These tests require the native library to be available.
 * They are disabled by default and can be enabled by setting the system property:
 * -Dtest.native.enabled=true
 */
public class DataFusionServiceTest {

    private DataFusionService service;
    private Environment testEnvironment;

    @Before
    public void setUp() throws IOException {
        // Create a temporary directory for test data
        Path tempDataDir = Files.createTempDirectory("opensearch-test-data");

        // Create test environment with the temp directory and path.repo setting
        Settings settings = Settings.builder().put("path.repo", tempDataDir.toString()).build();

        testEnvironment = new Environment(settings, null);

        // Create a dummy parquet file for testing
        Path parquetFile = tempDataDir.resolve("hits_data.parquet");
        Files.createFile(parquetFile);

        service = new DataFusionService(testEnvironment);
        service.doStart();
    }

    @Test
    public void testGetVersion() {
        String version = service.getVersion();
        assertNotNull(version);
        assertTrue(version.contains("datafusion_version"));
        assertTrue(version.contains("arrow_version"));
    }

    @Test
    public void testGetDefaultContext() {
        // Test that default context is created
        SessionContext defaultContext = service.getDefaultContext();
        assertNotNull(defaultContext);
        assertTrue(defaultContext.getContext() > 0);

        // Verify context exists
        SessionContext context = service.getContext(defaultContext.getContext());
        assertNotNull(context);
        assertEquals(defaultContext.getContext(), context.getContext());

        // Close context
        boolean closed = service.closeContext(defaultContext.getContext());
        assertTrue(closed);

        // Verify context is gone
        assertNull(service.getContext(defaultContext.getContext()));
    }
}
