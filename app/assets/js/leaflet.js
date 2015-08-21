/// Leaflet JS base map
///

//var map = L.map('map', { zoomControl:false }).setView([45.778425, -85.072381], 11);
var map = L.map('map', { zoomControl:false }).setView([27.757475, 85.296161], 11);

L.tileLayer ('http://{s}.tiles.mapbox.com/v3/azavea.map-zbompf85/{z}/{x}/{y}.png', 
	{
   	attribution: 'Raster Foundry | Map data &copy; <a href="http://openstreetmap.org">OpenStreetMap</a> contributors, <a href="http://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>, Imagery © <a href="http://mapbox.com">Mapbox</a>',
    maxZoom: 18
	}).addTo(map);

L.tileLayer ('http://localhost:8088/tms/{z}/{x}/{y}').addTo(map);
//L.tileLayer ('http://54.68.125.30/tms/{z}/{x}/{y}').addTo(map);


L.control.zoom ({
	position: 'bottomright'
}).addTo(map);
