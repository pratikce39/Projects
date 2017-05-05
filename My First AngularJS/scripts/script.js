
/// <reference path="angular.min.js" />

//Create the module
var myApp = angular.module("myModule", []);

// Creating the controller and registering with the module all done in one line.
myApp.controller("myController", function ($scope) { $scope.message = "AngularJS Tutorial";
}); 
