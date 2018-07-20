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

'use strict';

KylinApp.controller('HybridCubeSchema', function (
  $scope, $q, $location, $interpolate, $templateCache, $routeParams,
  CubeList, HybridCubeService, ProjectModel, modelsManager, SweetAlert, MessageService, loadingRequest, CubeService, CubeDescService
) {
  $scope.LEFT = 'LEFT';
  $scope.RIGHT = 'RIGHT';
  $scope.isFormDisabled = false;

  $scope.cubeList = CubeList;
  $scope.projectModel = ProjectModel;
  $scope.modelsManager = modelsManager;

  $scope.route = { params: $routeParams.hybridName };
  $scope.isEdit = !!$routeParams.hybridName;

  $scope.isEditInitialized = false;

  $scope.form = {
    uuid: '',
    name: '',
    model: ''
  };

  resetPageData();

  /**
   * Action: toggle all rows' check status of the table
   * 
   * @param {*} dir 
   */
  $scope.toggleAll = function(dir, toStatus) {
    var isCheckAll = $scope.isCheckAll(dir);
    var dataRows = $scope.table[dir].dataRows;

    dataRows.forEach(function(row) {
      if(row.model === $scope.form.model) {
        if(toStatus !== undefined) {
          row.isChecked = toStatus;
        } else {
          row.isChecked = !isCheckAll;
        }
      }
    });
  };

  /**
   * Action: transfer checked rows from destination table to source table
   * 
   * @param {*} dir 
   */
  $scope.transferTo = function(dir) {
    var toDir = dir;
    var fromDir = dir === $scope.RIGHT ? $scope.LEFT : $scope.RIGHT;
    var srcTable = $scope.table[fromDir];
    var disTable = $scope.table[toDir];

    // get checked rows from source table to transfer rows
    var transferRows = srcTable.dataRows.filter(function(row) {
      return row.isChecked;
    });

    // filter unchecked row to source table rows
    srcTable.dataRows = srcTable.dataRows.filter(function(row) {
      return !row.isChecked;
    });

    // clean transfer rows check status
    transferRows.forEach(function(row) {
      row.isChecked = false;
    });

    // push transfer rows to destination table
    disTable.dataRows = disTable.dataRows.concat(transferRows);
  }

  /**
   * Computed: judge that current cube row is checked
   * 
   * @param {*} dir 
   * @param {*} cube 
   */
  $scope.isCubeChecked = function(dir, cube) {
    return $scope.table[dir].checkedCubeIds.indexOf(function(cubeId) {
      return cubeId === cube.uuid;
    }) !== -1;
  };

  /**
   * Computed: judge that all rows of the table are checked
   * 
   * @param {*} dir 
   */
  $scope.isCheckAll = function(dir) {
    var dataRows = $scope.table[dir].dataRows;

    return dataRows.length ? dataRows.every(function(row) {
      return row.isChecked === true;
    }) : false;
  };

  /**
   * Computed: get the count of the model cubes
   * 
   * @param {*} dir 
   */
  $scope.getFiltedModelCubeCount = function(dir) {
    var dataRows = $scope.table[dir].dataRows;

    return dataRows.filter(function(row) {
      return row.model === $scope.form.model;
    });
  }

  /**
   * Computed: judge that model select component can be chosen
   */
  $scope.isModelSelectDisabled = function() {
    return !modelsManager.models.length
      || $scope.table[$scope.RIGHT].dataRows.length
      || $scope.table[$scope.LEFT].dataRows.some(function(row) {
        return row.isChecked;
      });
  }

  /**
   * Action: page edit cancel handler
   */
  $scope.cancel = function() {
    history.go(-1);
  };

  $scope.isFormValid = function() {
    var schema = getSchema();

    return Object.keys(schema).every(function(key) {
      return schema[key] instanceof Array ? schema[key].length : schema[key];
    });
  };

  /**
   * Action: page edit submit handler
   */
  $scope.submit = function() {
    // get form data
    var schema = getSchema();
    // show save warning
    saveWarning(function() {
      // show loading
      loadingRequest.show();
      // save the hybrid cube
      if(!$scope.isEdit) {
        HybridCubeService.save({}, schema, successHandler, failedHandler);
      } else {
        HybridCubeService.update({}, schema, successHandler, failedHandler);
      }
    });

    function successHandler(request) {
      if(request.successful === false) {
        var message = request.message;
        var msg = !!message ? message : 'Failed to take action.';
        var template = hybridCubeResultTmpl({ text: msg, schema: schema });
        MessageService.sendMsg(template, 'error', {}, true, 'top_center');
      } else {
        SweetAlert.swal('', 'Created hybrid cube successfully.', 'success');
        $location.path('/models');
      }
      loadingRequest.hide();
    }

    function failedHandler(e) {
      if (e.data && e.data.exception) {
        var message = e.data.exception;
        var msg = !!(message) ? message : 'Failed to take action.';
        var template = hybridCubeResultTmpl({ text: msg, schema: schema });
        MessageService.sendMsg(template, 'error', {}, true, 'top_center');
      } else {
        var template = hybridCubeResultTmpl({ text: 'Failed to take action.', schema: schema });
        MessageService.sendMsg(template, 'error', {}, true, 'top_center');
      }
      //end loading
      loadingRequest.hide();
    }
  }

  /**
   * Util: $watch extention method.
   * @param {*} watchers 
   * @param {*} callback 
   */
  $scope.$watchAll = function(watchers, callback) {
    var isChanged = false;

    watchers.forEach(function(watcher, index) {
      $scope.$watch(watcher, function(newValue, oldValue) {
        if(JSON.stringify(newValue) !== JSON.stringify(oldValue)) {
          isChanged = true;
        }
        if(watchers.length - 1 === index) {
          isChanged = false;
        }
        if(isChanged) {
          callback();
        }
      });
    });
  }

  doPerpare();

  function doPerpare() {
    $scope.$watch('projectModel.selectedProject', function (newValue, oldValue) {
      if (newValue != oldValue || newValue == null) {
        CubeList.removeAll();
        resetPageData();
        listModels();
      }
    });

    $scope.$watch('modelsManager.models', function() {
      $scope.form.model = modelsManager.models[0] && modelsManager.models[0].name || '';

      if ($scope.isEdit && !$scope.isEditInitialized) {
        getEditHybridCube();
        $scope.isEditInitialized = true;
      }
    });

    $scope.$watch('cubeList.cubes', function() {
      loadTableData();
    });
  }

  function getSchema() {
    const schema = {
      hybrid: $scope.form.name,
      project: $scope.projectModel.selectedProject,
      model: $scope.form.model,
      cubes: $scope.table[$scope.RIGHT].dataRows.map(function(row) {
        return row.name;
      })
    };

    if($scope.form.uuid) {
      schema.uuid = $scope.form.uuid;
    }
    return schema;
  }

  function resetPageData() {
    $scope.table = {};
    $scope.form.model = '';
    $scope.table[$scope.LEFT] = {
      dataRows: []
    };
    $scope.table[$scope.RIGHT] = {
      dataRows: []
    };
  }

  function listModels () {
    var defer = $q.defer();
    var queryParam = {};
    if (!$scope.projectModel.isSelectedProjectValid()) {
      defer.resolve([]);
      return defer.promise;
    }

    if (!$scope.projectModel.projects.length) {
      defer.resolve([]);
      return defer.promise;
    }
    queryParam.projectName = $scope.projectModel.selectedProject;
    return modelsManager.list(queryParam).then(function (resp) {
      defer.resolve(resp);
      modelsManager.loading = false;
      return defer.promise;
    });
  };

  function loadTableData() {
    var cubesData = Object.create($scope.cubeList.cubes);
    var usedCubeTable = $scope.table[$scope.RIGHT].dataRows;
    var unusedCubeTable = $scope.table[$scope.LEFT].dataRows = [];

    cubesData.forEach(function(cubeData) {
      var isInUsed = usedCubeTable.some(function(row) {
        return row.uuid === cubeData.uuid;
      });

      if(!isInUsed) {
        cubeData.isChecked = false;
        unusedCubeTable.push(cubeData);
      }
    });
  }

  function hybridCubeResultTmpl(notification) {
    // Get the static notification template.
    var tmpl = notification.type == 'success' ? 'hybridResultSuccess.html' : 'hybridResultError.html';
    console.log($templateCache.get(tmpl))
    return $interpolate($templateCache.get(tmpl))(notification);
  };

  function saveWarning(callback) {
    SweetAlert.swal({
      title: $scope.isEdit
        ? 'Are you sure to update the Hybrid Cube?'
        : 'Are you sure to save the Hybrid Cube?',
      text: $scope.isEdit
        ? ''
        : '',
      type: 'warning',
      showCancelButton: true,
      confirmButtonColor: '#DD6B55',
      confirmButtonText: "Yes",
      closeOnConfirm: true
  }, function(isConfirm) {
    if(isConfirm) {
      callback();
    }
  })};

  function getEditHybridCube() {
    HybridCubeService.getByName({ hybrid_name: $routeParams.hybridName }, function (hybirdCube) {
      var cubeList = [];
      $scope.form.uuid = hybirdCube.uuid;
      $scope.form.name = hybirdCube.name;

      hybirdCube.realizations.forEach(function(realizationItem) {
        CubeDescService.query({cube_name: realizationItem.realization}, {}, function(detail) {
          var cube = {
            uuid: '',
            name: detail[0].name,
            model: detail[0].model_name,
            status: '',
            project: ''
          };

          $scope.form.model = cube.model;

          CubeService.getCube({cubeId: realizationItem.realization}, {}, function(instance) {
            cube.uuid = instance.uuid;
            cube.status = instance.status;

            cubeList.push(cube);

            if(cubeList.length === hybirdCube.realizations.length) {
              initializeHybridUsedCube();
            }
          });
        });
      });

      function initializeHybridUsedCube() {
        cubeList.forEach(function(cube) {
          var rowCube = $scope.table[$scope.LEFT].dataRows.filter(function(row) {
            return row.uuid === cube.uuid;
          })[0];

          if(rowCube) {
            rowCube.isChecked = true;
          } else {
            cube.isChecked = true;
            $scope.table[$scope.LEFT].dataRows.push(cube);
          }
        });
  
        $scope.transferTo($scope.RIGHT);
      }
    });
  }
});