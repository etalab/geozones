<template>
<mapbox ref="map"
  :access-token="mapConfig.token"
  :map-options="mapConfig"
  @map-load="mapLoaded"
  >
</mapbox>
</template>

<script>
import {mapGetters, mapActions} from 'vuex'
import bbox from '@turf/bbox';
import Mapbox from 'mapbox-gl-vue'
import {isodate} from './helpers'


export default {
  components: {Mapbox},
  data() {
    return {hover: undefined}
  },
  computed: {
    map() {
      return this.$refs.map._map
    },
    ...mapGetters(['mapConfig', 'level', 'zone', 'date'])
  },
  methods: {
    mapLoaded(map) {
      map.addSource('zone', {type: 'geojson', data: {type: 'Feature'}})
      map.addLayer({
        id: 'zone',
        'type': 'line',
        'source': 'zone',
        'paint': {
          'line-color': 'rgba(255, 0, 0, 1)',
          'line-width': 3
        }
      })
    },
    loadLevel(levelId, at) {
      const isoAt = isodate(at)
      const url = `/levels/${levelId}@${isoAt}`

      if (this.map.getSource(levelId)) {
        this.map.getSource(levelId).setData(url)
        this.map.setLayoutProperty(levelId, 'visibility', 'visible')
      } else {
        this.map.addSource(levelId, {type: 'geojson', data: url})
        this.map.addLayer({
          id: levelId,
          type: 'fill',
          source: levelId,
          paint: {
            "fill-opacity": 0.4,
            'fill-color': {
              type: 'identity',
              property: 'color',
            },
            'fill-outline-color': 'rgba(200, 100, 240, 1)'
          },
        }, 'zone');
        this.map.on('click', levelId, this.onZoneClick)
      }
    },
    onZoneClick(evt) {
      const zone = evt.features[0]
      this.map.getSource('zone').setData(zone)
      this.setZone(zone.properties.id)
    },
    ...mapActions(['setZone'])
  },
  watch: {
    date(date) {
      this.loadLevel(this.level.id, date)
    },
    level(level, oldLevel) {
      if (oldLevel) this.map.setLayoutProperty(oldLevel.id, 'visibility', 'none')
      this.loadLevel(level.id, this.date)
    },
    zone(zone) {
      this.map.getSource('zone').setData(zone)
      this.map.fitBounds(bbox(zone), {padding: 50})
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
