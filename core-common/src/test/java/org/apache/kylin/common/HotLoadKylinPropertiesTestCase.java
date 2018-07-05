/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.kylin.common;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Properties;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Before;

/**
 * @author kangkaisen
 */

public class HotLoadKylinPropertiesTestCase extends LocalFileMetadataTestCase {
    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    protected void updateProperty(String key, String value) {
        File propFile = KylinConfig.getSitePropertiesFile();
        Properties conf = new Properties();

        //load
        try (final InputStream is = Files.newInputStream(propFile.toPath())) {
            conf.load(is);
            conf.setProperty(key, value);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }

        //store
        try (OutputStream out = Files.newOutputStream(propFile.toPath())) {
            conf.store(out, null);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}
