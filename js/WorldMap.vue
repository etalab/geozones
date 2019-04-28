<template>
<mapbox ref="map"
  :access-token="mapConfig.token"
  :map-options="mapConfig"
  >
</mapbox>
</template>

<script>
import {mapGetters, mapActions} from 'vuex'
import bbox from '@turf/bbox';
import Mapbox from 'mapbox-gl-vue'


export default {
  components: {Mapbox},
  data() {
    return {hover: undefined}
  },
  computed: {
    map() {
      return this.$refs.map._map
    },
    ...mapGetters(['mapConfig', 'level', 'levelUrl', 'zone'])
  },
  methods: {
    onZoneClick(evt) {
      this.setZone(evt.features[0])
    },
    onMouseEnter(evt) {
      this.map.getCanvas().style.cursor = 'pointer'
      this.hover = evt.features[0];
    },
    onMouseLeave(evt) {
      this.map.getCanvas().style.cursor = ''
      if (this.hover) {
        this.hover = undefined
      }
    },
    ...mapActions(['setZone'])
  },
  watch: {
    // Not working while mapbox-gl does not support string identifiers
    // hover(feature, old) {
    //   if (old) {
    //     this.map.setFeatureState(old, { hover: false})
    //   }
    //   if (feature) {
    //     this.map.setFeatureState(feature, { hover: true})
    //   }
    // },
    level(level, oldLevel) {
      this.hover = undefined
      const url = `/levels/${level.id}`
      if (oldLevel) {
        console.log('old level', oldLevel, oldLevel.id)
        this.map.setLayoutProperty(oldLevel.id, 'visibility', 'none')
      }

      if (this.map.getSource(level.id)) {
        this.map.getSource(level.id).setData(url)
        this.map.setLayoutProperty(level.id, 'visibility', 'visible')
      } else {
        this.map.addSource(level.id, {type: 'geojson', data: url})
        this.map.addLayer({
          id: level.id,
          type: 'fill',
          source: level.id,
          paint: {
            "fill-opacity": 0.4,
            // Not possible while string identifier are not supported by mapbox-gl
            // 'fill-opacity': ['case',
            //   ['boolean', ['feature-state', 'hover'], false],
            //   1,
            //   0.5
            // ],
            'fill-color': {
              type: 'identity',
              property: '_color',
            },
            'fill-outline-color': 'rgba(200, 100, 240, 1)'
          },
        });
        this.map.on('click', level.id, this.onZoneClick)
        this.map.on('mouseenter', level.id, this.onMouseEnter)
        this.map.on('mouseleave', level.id, this.onMouseLeave)
      }
    },
    zone(zone) {
      this.map.fitBounds(bbox(zone), {padding: 20})
    }
  }
}
</script>

<style scoped>
.mapboxgl-map {
  width: 100%;
  height: 100%;
}
</style>
