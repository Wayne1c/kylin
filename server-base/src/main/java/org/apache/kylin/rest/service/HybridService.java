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

package org.apache.kylin.rest.service;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.RealizationEntry;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.rest.job.HybridCubeCLI;
import org.apache.kylin.rest.response.HybridRespone;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.storage.hybrid.HybridInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component("hybridService")
public class HybridService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(HybridService.class);

    @Autowired
    private AclEvaluate aclEvaluate;

    public HybridRespone createHybridCube(String hybridName, String projectName, String modelName,
            String[] cubeNames) {
        aclEvaluate.checkProjectWritePermission(projectName);
        List<String> args = new ArrayList<String>();
        args.add("-name");
        args.add(hybridName);
        args.add("-project");
        args.add(projectName);
        args.add("-model");
        args.add(modelName);
        args.add("-cubes");
        args.add(StringUtils.join(cubeNames, ","));
        args.add("-action");
        args.add("create");
        try {
            HybridCubeCLI.main(args.toArray(new String[args.size()]));
        } catch (Exception e) {
            logger.warn("Create Hybrid Failed", e);
            throw e;
        }
        return getHybridInstance(hybridName);
    }

    public HybridRespone updateHybridCube(String hybridName, String projectName, String modelName,
            String[] cubeNames) {
        aclEvaluate.checkProjectWritePermission(projectName);
        List<String> args = new ArrayList<String>();
        args.add("-name");
        args.add(hybridName);
        args.add("-project");
        args.add(projectName);
        args.add("-model");
        args.add(modelName);
        args.add("-cubes");
        args.add(StringUtils.join(cubeNames, ","));
        args.add("-action");
        args.add("update");
        try {
            HybridCubeCLI.main(args.toArray(new String[args.size()]));
        } catch (Exception e) {
            logger.warn("Update Hybrid Failed", e);
            throw e;
        }
        return getHybridInstance(hybridName);
    }

    public void deleteHybridCube(String hybridName, String projectName) {
        aclEvaluate.checkProjectWritePermission(projectName);
        List<String> args = new ArrayList<String>();
        args.add("-name");
        args.add(hybridName);
        args.add("-action");
        args.add("delete");
        try {
            HybridCubeCLI.main(args.toArray(new String[args.size()]));
        } catch (Exception e) {
            logger.warn("Delete Hybrid Failed", e);
            throw e;
        }
    }

    public HybridRespone getHybridInstance(String hybridName) {
        HybridInstance hybridInstance = getHybridManager().getHybridInstance(hybridName);
        DataModelDesc modelDesc = hybridInstance.getModel();
        return new HybridRespone(modelDesc.getProject(), modelDesc.getName(), hybridInstance);
    }

    public List<HybridRespone> listHybrids(final String projectName, final String modelName) {
        List<HybridRespone> allHybrids = new ArrayList<HybridRespone>();
        if (StringUtils.isEmpty(projectName)) {
            List<ProjectInstance> allProjectInstances = getProjectManager().listAllProjects();
            List<ProjectInstance> readableProjects = new ArrayList<ProjectInstance>();
            for (ProjectInstance projectInstance : allProjectInstances) {
                if (projectInstance == null) {
                    continue;
                }
                boolean hasProjectReadAccess = aclEvaluate.hasProjectReadPermission(projectInstance);
                if (hasProjectReadAccess) {
                    readableProjects.add(projectInstance);
                }
            }
            for (ProjectInstance projectInstance : readableProjects) {
                List<RealizationEntry> realizationEntries = projectInstance
                        .getRealizationEntries(RealizationType.HYBRID);
                if (realizationEntries != null) {
                    for (RealizationEntry entry : realizationEntries) {
                        HybridInstance instance = getHybridManager().getHybridInstance(entry.getRealization());
                        DataModelDesc modelDesc = instance.getModel();
                        allHybrids.add(new HybridRespone(modelDesc == null ? HybridRespone.NO_PROJECT : modelDesc.getProject(), modelDesc == null ? HybridRespone.NO_MODEL : modelDesc.getName(), instance));
                    }
                }
            }
        } else {
            aclEvaluate.checkProjectReadPermission(projectName);
            ProjectInstance projectInstance = getProjectManager().getProject(projectName);
            List<RealizationEntry> realizationEntries = projectInstance.getRealizationEntries(RealizationType.HYBRID);
            if (realizationEntries != null) {
                for (RealizationEntry entry : realizationEntries) {
                    HybridInstance instance = getHybridManager().getHybridInstance(entry.getRealization());
                    DataModelDesc modelDesc = instance.getModel();
                    allHybrids.add(new HybridRespone(projectName, modelDesc == null ? HybridRespone.NO_MODEL : modelDesc.getName(), instance));
                }
            }
        }

        if (StringUtils.isEmpty(modelName)) {
            return allHybrids;
        } else {
            DataModelDesc model = getDataModelManager().getDataModelDesc(modelName);
            List<HybridRespone> hybridsInModel = new ArrayList<HybridRespone>();
            if (model == null)
                return hybridsInModel;

            for (HybridRespone hybridRespone : allHybrids) {
                boolean hybridInModel = false;
                for (RealizationEntry entry : hybridRespone.getHybridInstance().getRealizationEntries()) {
                    CubeDesc cubeDesc = getCubeDescManager().getCubeDesc(entry.getRealization());
                    if (cubeDesc != null && model.getName().equalsIgnoreCase(cubeDesc.getModel().getName())) {
                        hybridInModel = true;
                        break;
                    }
                }
                if (hybridInModel) {
                    hybridsInModel.add(hybridRespone);
                }
            }
            return hybridsInModel;
        }
    }

}
