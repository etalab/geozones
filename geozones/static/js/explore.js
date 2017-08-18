'use strict';

var LEVELS_URL = '/levels',
    LEVEL_URL = '/levels/{level}',
    STYLE = {
        clickable: true,
        weight: 0.5,
        opacity: 0.5,
        fillOpacity: 0.3
    },
    HOVER_STYLE = {
        fillOpacity: 0.8
    },
    HIDPI = (window.devicePixelRatio > 1 || (
        window.matchMedia
        && window.matchMedia('(-webkit-min-device-pixel-ratio: 1.25),(min-resolution: 120dpi)').matches)
    ),
    ATTRIBUTIONS = '&copy;' + [
        '<a href="http://openstreetmap.org/copyright">OpenStreetMap</a>',
        '<a href="https://cartodb.com/attributions">CartoDB</a>'
    ].join('/'),
    TILES_URL = `https://cartodb-basemaps-{s}.global.ssl.fastly.net/light_all/{z}/{x}/{y}${HIDPI ? '@2x' : ''}.png`,
    TILES_CONFIG = {subdomains: 'abcd', attribution: ATTRIBUTIONS},
    $el = $('.map-container'),
    map, levels, current_layer, sidebar;

function sort_levels(a, b) {
    return a.position - b.position;
}

function random_color() {
    var letters = '0123456789ABCDEF'.split(''),
        color = '#';

    for (var i = 0; i < 6; i++) {
        color += letters[Math.round(Math.random() * 15)];
    }
    return color;
}

function layer_url(level) {
    return LEVEL_URL.replace('{level}', level.id);
}

function on_levels_loaded(data) {
    var layers = {};

    levels = data.map(function(level) {
        var layer = L.geoJson(null, {
            style: STYLE,
            onEachFeature: on_each_feature
        });
        level.data_url = layer_url(level);
        level.layer = layer;

        layer.level = level;
        layers[level.label] = layer;
        return level;
    });

    L.control.layers(layers, null, {collapsed: false}).addTo(map);
    map.on('baselayerchange', function(ev) {
        switch_level(ev.layer.level);
    });

    switch_level(levels[0]);
}

/**
 * Display a given level
 */
function switch_level(level) {
    if (typeof level == 'string' || level instanceof String) {
        level = levels.filter(function(current_level) {
            return current_level.id == level;
        })[0];
    }

    if (level.layer == current_layer) {
        // already set
        return;
    }

    if (current_layer && map.hasLayer(current_layer)) {
        current_layer.clearLayers();
        map.removeLayer(current_layer);
    }
    current_layer = level.layer;
    if (!map.hasLayer(level.layer)) {
        level.layer.addTo(map);
    }

    if (!level.layer.loaded) {
        $.get(level.data_url, function(data) {
            level.layer.loaded = true;
            current_layer.addData(data);
        });
    }
}

/**
 * Apply style to each feature:
 */
function on_each_feature(feature, layer) {
    var base_color = random_color();

    layer.setStyle($.extend({}, STYLE, {
        color: base_color,
        fillColor: base_color
    }));

    layer.on('mouseover', function() {
        layer.setStyle(HOVER_STYLE);
    });
    layer.on('mouseout', function() {
        layer.setStyle(STYLE);
    });

    layer.on('click', display_feature);

    // Layer to the back on right click
    layer.on('contextmenu', function() {
        layer.bringToBack();
    })
}

function display_feature(ev) {
    var feature = ev.target.feature,
        $sidebar = $('#sidebar'),
        keys;

    keys = Object.keys(feature.properties.keys).map(function(key) {
        return key + ': ' + feature.properties.keys[key];
    });

    sidebar.show();

    $sidebar.find('#idfield').val(feature.id);
    $sidebar.find('#title').text(feature.properties.name);
    $sidebar.find('#namefield').val(feature.properties.name);
    $sidebar.find('#levelfield').val(feature.properties.level);
    $sidebar.find('#codefield').val(feature.properties.code);
    $sidebar.find('#areafield').val(feature.properties.area);
    $sidebar.find('#populationfield').val(feature.properties.population);
    $sidebar.find('#parentsfield')
        .attr('rows', feature.properties.parents.length)
        .val(feature.properties.parents.join('\n'));

    $sidebar.find('#keysfield')
        .attr('rows', keys.length)
        .val(keys.join('\n'));
}

function on_feature_popup_click(ev, feature) {
    switch_level($(ev.target).data('level'));
    return false;
}


$(function() {
    // Initialize the map
    map = L.map($el[0], {center: [42, 2.4], zoom: 4});
    L.tileLayer(TILES_URL, TILES_CONFIG).addTo(map);

    sidebar = L.control.sidebar('sidebar', {
        position: 'right'
    });

    map.addControl(sidebar);

    // Fetch levels
    $.get(LEVELS_URL, on_levels_loaded);
});
