 
function ShapesMap(_mapContainer, _deleteButton, _clearButton, _console, __myLocationIcon) {
 
    // state
 
    var _selection = null;
    var _map = null;
    var _drawingManager = null;
    var _newShapeNextId = 0;
    var _shapes = Array();
 
    // types
 
    var RECTANGLE = google.maps.drawing.OverlayType.RECTANGLE;
    var CIRCLE = google.maps.drawing.OverlayType.CIRCLE;
    var POLYGON = google.maps.drawing.OverlayType.POLYGON;
    var POLYLINE = google.maps.drawing.OverlayType.POLYLINE;
    var MARKER = google.maps.drawing.OverlayType.MARKER; 
 
    function typeDesc(type) {
        switch (type) {
        case RECTANGLE:
            return "rectangle";
 
        case CIRCLE:
            return "circle";
 
        case POLYGON:
            return "polygon";
 
        case POLYLINE:
            return "polyline";
 
        case MARKER:
            return "marker";
 
        case null:
            return "null";
 
        default:
            return "UNKNOWN GOOGLE MAPS OVERLAY TYPE";
        }
    }
 
    // json reading
 
    function jsonReadPath(jsonPath) {
        var path = new google.maps.MVCArray();
 
        for (var i = 0; i < jsonPath.path.length; i++) {
            var latlon = 
                new google.maps.LatLng(jsonPath.path[i].lat, jsonPath.path[i].lon);
            path.push(latlon);
        }
 
        return path;
    }
 
    function jsonReadRectangle(jsonRectangle) {
        var jr = jsonRectangle;
        var southWest = new google.maps.LatLng(
            jr.bounds.southWest.lat, 
            jr.bounds.southWest.lon);
        var northEast = new google.maps.LatLng(
            jr.bounds.northEast.lat, 
            jr.bounds.northEast.lon);
        var bounds = new google.maps.LatLngBounds(southWest, northEast);
 
        var rectangleOptions = {
            bounds: bounds,
            editable: false,
            fillColor: jr.color,
            map: _map
        };
     
        var rectangle = new google.maps.Rectangle(rectangleOptions);
 
        return rectangle;
    }
 
    function jsonReadCircle(jsonCircle) {
        var jc = jsonCircle;
 
        var center = new google.maps.LatLng(
            jc.center.lat, 
            jc.center.lon);
 
        var circleOptions = {
            center: center,
            radius: parseFloat(jc.radius),
            editable: false,
            fillColor: jc.color,
            map: _map
        };
     
        var circle = new google.maps.Circle(circleOptions);
 
        return circle;
    }
 
    function jsonReadPolyline(jsonPolyline) {
        var path = jsonReadPath(jsonPolyline);
 
        var polylineOptions = {
            path: path, 
            editable: false,
            strokeColor: jsonPolyline.color,
            map: _map
        };
 
        var polyline = new google.maps.Polyline(polylineOptions);
 
        return polyline;
    }
 
    function jsonReadPolygon(jsonPolygon) {
        var paths = new google.maps.MVCArray();
 
        for (var i = 0; i < jsonPolygon.paths.length; i++) {
            var path = jsonReadPath(jsonPolygon.paths[i]);
            paths.push(path);
        }
 
        var polygonOptions = {
            paths: paths, 
            editable: false,
            fillColor: jsonPolygon.color,
            map: _map
        };
 
        var polygon = new google.maps.Polygon(polygonOptions);
 
        return polygon;
    }
 
    function jsonRead(json) { 
        var jsonObject = eval("(" + json + ")");
 
        for (i = 0; i < jsonObject.shapes.length; i++)
        {
            switch (jsonObject.shapes[i].type) {
            case RECTANGLE:
                var rectangle = jsonReadRectangle(jsonObject.shapes[i]);
                newShapeSetProperties(rectangle, RECTANGLE);
                newShapeAddListeners(rectangle);
                shapesAdd(rectangle);
                break;
 
            case CIRCLE:
                var circle = jsonReadCircle(jsonObject.shapes[i]);
                newShapeSetProperties(circle, CIRCLE);
                newShapeAddListeners(circle);
                shapesAdd(circle);
                break;
 
            case POLYLINE:
                var polyline = jsonReadPolyline(jsonObject.shapes[i]);
                newShapeSetProperties(polyline, POLYLINE);
                newShapeAddListeners(polyline);
                shapesAdd(polyline);
                break;
 
            case POLYGON:
                var polygon = jsonReadPolygon(jsonObject.shapes[i]);
                newShapeSetProperties(polygon, POLYGON);
                newShapeAddListeners(polygon);
                shapesAdd(polygon);
                break;
            }
        }
    }
 
    // json writing
 
    function comma(i) {
        return (i > 0) ? ',' : '';
    }
 
    function jsonMakeLatlon(latlon) {
        var buf = 
            '"lat":"' + latlon.lat() + '","lon":"' + latlon.lng() + '"';
            // '"' + latlon.lat() + '","' + latlon.lng() + '"';

        return buf;
    }
 
    function jsonMakeBounds(bounds) {
        var buf = 
            '"bounds":{'
            + '"northEast":{' + jsonMakeLatlon(bounds.getNorthEast()) + '},'
            + '"southWest":{' + jsonMakeLatlon(bounds.getSouthWest()) + '}'
            + '}';
 
        return buf;
    }
 
    function jsonMakeType(type) {
        var buf = '"type":"' + typeDesc(type) + '"';
 
        return buf;
    }
 
    function jsonMakeColor(color) {
        var buf = '"color":"' + color + '"';
 
        return buf;
    }
 
    function jsonMakeCenter(center) {
        var buf = '"center":{' + jsonMakeLatlon(center) + '}';
 
        return buf;
    }
 
    function jsonMakeRadius(radius) {
        var buf = '"radius":"' + radius + '"';
 
        return buf;
    }
 
    function jsonMakePath(path) {
        var n = path.getLength();
 
        var buf = '"path":[';
        // var buf = '"coordinates":[';
        for (var i = 0; i < n; i++) {
            var latlon = path.getAt(i);
 
            buf += comma(i) + '{' + jsonMakeLatlon(latlon) + '}';
            // buf += comma(i) + '[' + jsonMakeLatlon(latlon) + ']';
        }
        buf += ']';
 
        return buf;
    }
 
    function jsonMakePaths(paths) {
        var n = paths.getLength();
 
        var buf = '"paths":[';
        // var buf = '"geometry":';
        for (var i = 0; i < n; i++) {
            var path = paths.getAt(i);
 
            buf += comma(i) + '{' + jsonMakePath(path) + '}';
            // buf += comma(i) + '{' + jsonMakeType(POLYGON) + ',' + jsonMakePath(path) + '}';
        }
        buf += ']';
 
        return buf;
    }
 
    function jsonMakeRectangle(rectangle) {
        var buf = 
            jsonMakeType(RECTANGLE) + ','
            + jsonMakeColor(rectangle.fillColor) + ','
            + jsonMakeBounds(rectangle.bounds);
 
        return buf;
    }
 
    function jsonMakeCircle(circle) {
        var buf = 
            jsonMakeType(CIRCLE) + ','
            + jsonMakeColor(circle.fillColor) + ','
            + jsonMakeCenter(circle.center) + ','
            + jsonMakeRadius(circle.radius);
 
        return buf;
    }
 
    function jsonMakePolyline(polyline) {
        var buf = 
            jsonMakeType(POLYLINE) + ','
            + jsonMakeColor(polyline.strokeColor) + ','
            + jsonMakePath(polyline.getPath());
 
        return buf;
    }
 
    function jsonMakePolygon(polygon) {
        var buf = 
            jsonMakeType(POLYGON) + ','
            + jsonMakeColor(polygon.fillColor) + ','
            + jsonMakePaths(polygon.getPaths());
            
            // jsonMakePaths(polygon.getPaths()); 
            trace(myGetArea(polygon) + ' : acre');

            // polygon.toGeoJson(function(geoJson){
            //     trace(geoJson);
            // });
        return buf;
    }
 
    function jsonMake() {
        var buf = '{"shapes":[';
        // var buf = '{"type": "Feature",';
        for (i = 0; i < _shapes.length; i++) {
            switch (_shapes[i].type)
            {
            case RECTANGLE:
                buf += comma(i) + '{' + jsonMakeRectangle(_shapes[i]) + '}';
                break;
 
            case CIRCLE:
                buf += comma(i) + '{' + jsonMakeCircle(_shapes[i]) + '}';
                break;
 
            case POLYLINE:
                buf += comma(i) + '{' + jsonMakePolyline(_shapes[i]) + '}';
                break;
 
            case POLYGON:
                buf += comma(i) + '{' + jsonMakePolygon(_shapes[i]) + '}';
                break;
            }
        }
        buf += ']}';
        // buf += '}';
        return buf;
    }
 
    // storage
    // var mylayer = new google.maps.Data();
    // mylayer.setMap(_map);
    function shapesAdd(shape) {
        _shapes.push(shape);
        // trace(shape);
        // trace(shape.getPaths);
        // trace('Here');
        // trace(_map.data);
        // mylayer.add(shape);
        // trace(mylayer);
        
        // _map.data.toGeoJson(function(geoJson){
        //     trace(geoJson);
        // });         
        
    }
 
    function shapesDelete(shape) {
        var found = false;
 
        for (var i = 0; i < _shapes.length && !found; i++) {
            if (_shapes[i] === shape) {
                _shapes.splice(i, 1);
                found = true;
            }
        }
    }
 
    function shapesHideAll() {
        for (var i = 0; i < _shapes.length; i++) {
            _shapes[i].setMap(null);
        }
    }
 
    function shapesDeleteAll() {
        print(_shapes.length + " shapes deleted\n");
 
        _shapes.splice(0, _shapes.length);
    }
 
    function shapesSave() { 
        var shapes = jsonMake();
        trace('shapes: ' + shapes);

        var expirationDate = new Date();
        expirationDate.setDate(expirationDate.getDate + 365);
         
        var value = escape(shapes) 
            + "; expires=" + expirationDate.toUTCString();
        document.cookie = "shapes=" + value;
    }
 
    function shapesLoad() {
        var start_length = _shapes.length;
 
        var cookies = document.cookie.split(";");
        for (var i = 0; i < cookies.length; i++) {
            var key = cookies[i].substr(0, cookies[i].indexOf("="));
            key = key.replace("/^\s+|\s+$/g", "");
 
            if (key == "shapes") {
                var value = cookies[i].substr(cookies[i].indexOf("=") + 1);
 
                jsonRead(unescape(value));
            }
        }
 
        var n_loaded = _shapes.length - start_length;
        print(n_loaded + " shapes loaded\n");
    }
 
    // printing
 
    function print(string) {
        _console.innerHTML += string;
        _console.scrollTop = _console.scrollHeight;
    }
 
    function printDrawingMode(drawingManager) {
        print(
            "drawing mode set to "
            + typeDesc(drawingManager.getDrawingMode())
            + "\n");
    }
 
    // selection
 
    function selectionPrint() {
        if (_selection == null) {
            print("selection cleared\n");
        }
        else {
            print(_selection.appId + ": selected\n");
        }
    }
 
    function selectionIsSet() {
        return _selection != null;
    }
 
    function selectionSet(newSelection) {
        if (newSelection == _selection) {
            return;
        }
 
        if (_selection != null) {
            _selection.setEditable(false);
            _selection = null;
        }
 
        if (newSelection != null) {
            _selection = newSelection;
            _selection.setEditable(true);
        }
 
        selectionPrint();
    }
 
    function selectionClear() {
        selectionSet(null);
    }
 
    function selectionDelete() {
        if (_selection != null) {
            _selection.setMap(null);
            selectionClear();
        }
    }
 
    // new shape integration
 
    function newShapeAddPathListeners(shape, path) {
        google.maps.event.addListener(
            path, 
            'insert_at', 
            function () {onShapeEdited(shape)});
        google.maps.event.addListener(
            path, 
            'remove_at',
            function () {onShapeEdited(shape)});
        google.maps.event.addListener(
            path, 
            'set_at',
            function () {onShapeEdited(shape)});
    }
 
    function newShapeAddListeners(shape) {
        google.maps.event.addListener(
            shape, 
            'click', 
            function () {onShapeClicked(shape);});
 
        switch (shape.type) {
        case RECTANGLE:
            google.maps.event.addListener(
                shape, 
                'bounds_changed', 
                function () {onShapeEdited(shape);});
            break;
 
        case CIRCLE:
            google.maps.event.addListener(
                shape,
                'center_changed',
                function () {onShapeEdited(shape);});
            google.maps.event.addListener(
                shape,
                'radius_changed',
                function () {onShapeEdited(shape);});
            break;
 
        case POLYLINE:
            var path = shape.getPath();
            newShapeAddPathListeners(shape, path);
            break;
 
        case POLYGON:
            var paths = shape.getPaths();
 
            var n = paths.getLength();
            for (var i = 0; i < n; i++) {
                var path = paths.getAt(i);
                newShapeAddPathListeners(shape, path);
            }
            break;
        }
    }
 
    function newShapeSetProperties(shape, type) {
        shape.type = type;
        shape.appId = _newShapeNextId;
 
        _newShapeNextId++;
    }
 
    // map creation
  
    function createMap(mapContainer) {
        var center = new google.maps.LatLng(45.00495,-90.00052);
  
        var mapOptions = {
            zoom: 16,
            center: center,
            mapTypeId: google.maps.MapTypeId.HYBRID,
            // disableDefaultUI: true,
            zoomControl: true,
            scaleControl: true
        };
      
        var map = new google.maps.Map(mapContainer, mapOptions);

        google.maps.event.addListener(map, 'click', onMapClicked); 

        return map;
    }
 
    // drawing manager creation
 
    function drawingManagerCreate() {
 
        // create drawing manager
 
        // var drawingModes = new Array(
        //     RECTANGLE, CIRCLE, POLYGON, POLYLINE);
        var drawingModes = new Array(
            POLYGON);
 
        var drawingControlOptions = {
            drawingModes: drawingModes,
            position: google.maps.ControlPosition.TOP_CENTER
        };
 
        var polyOptions = {
            editable: true,
            draggable: true,
            fillColor: '#cccccc',
            fillOpacity: 0.5,
            strokeColor: '#000000'        
        };
 
        drawingManagerOptions = {
            drawingMode: null,
            drawingControlOptions: drawingControlOptions,
            markerOptions: { draggable: true },
            polylineOptions: { editable: true },
            rectangleOptions: polyOptions,
            circleOptions: polyOptions,
            polygonOptions: polyOptions,
            map: _map
        };
 
        drawingManager = new google.maps.drawing.DrawingManager(
            drawingManagerOptions);
 
        // tie events to map
 
        google.maps.event.addListener(
            drawingManager, 
            'overlaycomplete', 
            onNewShape);
        google.maps.event.addListener(
            drawingManager, 
            'drawingmode_changed',
            onDrawingModeChanged);
 
        // print initial drawing mode, selection
 
        printDrawingMode(drawingManager); 
        selectionPrint();
 
        return drawingManager;
    }
 
    // event capture
 
    function onNewShape(event) {
        var shape = event.overlay;

        newShapeSetProperties(shape, event.type);
        newShapeAddListeners(shape);
        shapesAdd(shape);
        shapesSave();
        selectionSet(shape);
 
        print("new " + typeDesc(event.type) + " created (id = "
              + shape.appId + ")\n");
    }
 
    function onShapeEdited(shape) {
        print(shape.appId + ": shape edited\n");
        shapesSave();
    }
 
    function onShapeClicked(shape) {
        print(shape.appId + ": shape clicked\n");
        selectionSet(shape);
    }
 
    function onMapClicked() {
        print("map clicked\n");
        selectionClear();
    }
 
    function onDeleteButtonClicked() {
        print("delete button clicked\n");
 
        if (selectionIsSet()) {
            shapesDelete(_selection);
            shapesSave();
            selectionDelete();
        }
    }
 
    function onClearButtonClicked() {
        print("clear button clicked\n");
 
        selectionClear();
        shapesHideAll();
        shapesDeleteAll();
        shapesSave();
    }
 
    function onDrawingModeChanged() {
        printDrawingMode(drawingManager); 
        selectionClear();
    }
 
    function onCreate() {
        _map = createMap(_mapContainer);
        _drawingManager = drawingManagerCreate(_map);
        // Create the DIV to hold the control and
        // call the MyLocationControl() constructor passing
        // in this DIV.
        var myLocationControlDiv = document.createElement('div');
        var myLocationCon =  new myLocationControl(myLocationControlDiv);

        myLocationControlDiv.index = 1;
        _map.controls[google.maps.ControlPosition.LEFT_CENTER].push(myLocationControlDiv, _map); 
        //Add the searchbox
        mySearchBox(_map);            


        google.maps.event.addDomListener(
            _deleteButton, 
            'click', 
            onDeleteButtonClicked);
        google.maps.event.addDomListener(
            _clearButton, 
            'click', 
            onClearButtonClicked);
 
        shapesLoad();
    }

    //trace function for debugging
    function trace(message) {
        if (typeof console != 'undefined') {
          console.log(message);
        };
    }

    /**
    *My Location COntrol
    */ 
    function myLocationControl(controlDiv, map) {
        // Set CSS for the control border
        var controlUI = document.createElement('div');
        controlUI.style.backgroundColor = '#fff';
        controlUI.style.border = '2px solid #fff';
        controlUI.style.borderRadius = '3px';
        controlUI.style.boxShadow = '0 2px 6px rgba(0,0,0,.3)';
        controlUI.style.cursor = 'pointer';
        controlUI.style.marginLeft = '32px';
        controlUI.style.textAlign = 'center';
        controlUI.title = 'My Location';
        controlDiv.appendChild(controlUI);
        // Set CSS for the control interior
        var controlImage = document.createElement('div');
        controlImage.style.width = '16px';
        controlImage.style.height = '16px';
        // trace("{{ url_for('static', filename='images/icon-locate-me.png') }}");
        controlImage.innerHTML = '<img src= ' + __myLocationIcon + ' />';
        controlUI.appendChild(controlImage);
        // Setup the click event listeners: simply set the map to
        // My Location
        google.maps.event.addDomListener(controlUI, 'click', function() {
            myAutoLocate();
        });        
    }

    /**
    *My Location
    */
    function myAutoLocate() {
        var latlng;
        //Automaticallly findout out the location
        if (navigator.geolocation) {
            navigator.geolocation.getCurrentPosition(success, failure);
            trace("In Geolocation");
        } 
        else {
            trace('Geo Location is not supported');
        }

        function success(position) {
            latlng = new google.maps.LatLng(position.coords.latitude, position.coords.longitude); 
            trace('Success: ' + position.coords.latitude + ' ' + position.coords.longitude);
            _map.setCenter(latlng);
            _map.setZoom(10);
            myDrawCircle(latlng, 1000);
        }

        function failure() {
            trace('Failure');
        }          
    }

    //draw circle  
    function myDrawCircle(centerlatlangobj, radius) {
        var bufferOptions = {
            strokeColor: "#FF0000",
            strokeOpacity: 0.8,
            strokeWeight: 2,
            fillColor: "#FF0000",
            fillOpacity: 0.35,
            map: _map,
            center: centerlatlangobj,
            radius: radius
        };                        
        myLocCircle = new google.maps.Circle(bufferOptions);               
    }  
    /**
    *Search Box
    */ 
    function mySearchBox(map) {
        var markers = [];
        // Create the search box and link it to the UI element.
        var input = /** @type {HTMLInputElement} */(
          document.getElementById('pac-input'));
        map.controls[google.maps.ControlPosition.TOP_LEFT].push(input);

        var searchBox = new google.maps.places.SearchBox(
        /** @type {HTMLInputElement} */(input));

        // Listen for the event fired when the user selects an item from the
        // pick list. Retrieve the matching places for that item.
        google.maps.event.addListener(searchBox, 'places_changed', function() {
            var places = searchBox.getPlaces();

            if (places.length == 0) {
              return;
            }
            for (var i = 0, marker; marker = markers[i]; i++) {
              marker.setMap(null);
            }

            // For each place, get the icon, place name, and location.
            markers = [];
            var bounds = new google.maps.LatLngBounds();
            for (var i = 0, place; place = places[i]; i++) {
                var image = {
                    url: place.icon,
                    size: new google.maps.Size(71, 71),
                    origin: new google.maps.Point(0, 0),
                    anchor: new google.maps.Point(17, 34),
                    scaledSize: new google.maps.Size(25, 25)
                };

                // Create a marker for each place.
                var marker = new google.maps.Marker({
                    map: map,
                    icon: image,
                    title: place.name,
                    position: place.geometry.location
                });

                markers.push(marker);

                bounds.extend(place.geometry.location);
            }
            map.fitBounds(bounds);
            map.setZoom(5);
        });

        // Bias the SearchBox results towards places that are within the bounds of the
        // current map's viewport.
        google.maps.event.addListener(map, 'bounds_changed', function() {
            var bounds = map.getBounds();
            searchBox.setBounds(bounds);
        });            
    }

    /**
    * Get area of the drawn polygon in acres
    */
    function myGetArea(poly) {
        var result = parseFloat(google.maps.geometry.spherical.computeArea(poly.getPath())) * 0.000247105;
        return result.toFixed(4);
    }     
    // initialization
 
    onCreate();
}