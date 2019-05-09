<template>
<mapbox ref="map"
  :access-token="mapConfig.token"
  :map-options="mapConfig"
  @map-load="mapLoaded"
  @map-error="onError"
  @map-data="onData"
  >
</mapbox>
</template>

<script>
import {mapActions, mapState, mapMutations} from 'vuex'
import bbox from '@turf/bbox';
import Mapbox from 'mapbox-gl-vue'
import {isodate} from './helpers'

export default {
  components: {Mapbox},
  data() {
    return {hover: undefined, dataDownloaded: false, popup: undefined}
  },
  computed: {
    map() {
      return this.$refs.map._map
    },
    ...mapState(['mapConfig', 'level', 'zone', 'date'])
  },
  methods: {
    mapLoaded(map) {
      this.mapLoading(false)
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

      this.startLoading()
      if (this.map.getSource(levelId)) {
        this.map.getSource(levelId).setData(url)
        this.map.setLayoutProperty(levelId, 'visibility', 'visible')
      } else {
        this.map.addSource(levelId, {type: 'geojson', data: url, generateId: true})
        this.map.addLayer({
          id: levelId,
          type: 'fill',
          source: levelId,
          paint: {
            // 'fill-opacity': 0.4,
            'fill-color': {
              type: 'identity',
              property: 'color',
            },
            'fill-opacity': ['case',
              ['boolean', ['feature-state', 'hover'], false],
              0.7, 0.4
            ],
            'fill-outline-color': 'rgba(200, 100, 240, 1)'
          },
        }, 'zone');
        this.map.on('click', levelId, this.onZoneClick)
        this.map.on("mousemove", levelId, this.onMouseMove)
        this.map.on("mouseleave", levelId, this.onMouseLeave)
      }
    },
    onZoneClick(evt) {
      const zone = evt.features[0]
      this.map.getSource('zone').setData(zone)
      this.setZone(zone.properties.id)
    },
    onMouseMove(e) {
      if (e.features.length <= 0) return
      if (this.hover) {
        this.map.setFeatureState({source: this.level.id, id: this.hover}, {hover: false})
      }
      const feature = e.features[0]
      this.hover = feature.id
      this.map.setFeatureState({source: this.level.id, id: this.hover}, {hover: true})
      this.map.getCanvas().style.cursor = 'pointer'
    },
    onMouseLeave(e) {
      if (this.hover) {
        this.map.setFeatureState({source: this.level.id, id: this.hover}, {hover: false})
      }
      this.hover = null
      this.map.getCanvas().style.cursor = ''
    },
    startLoading() {
      this.dataDownloaded = false
      this.mapLoading(true)
    },
    onData(e) {
      if (this.level && this.map.isSourceLoaded(this.level.id) && this.map.isStyleLoaded()) {
        if (!this.dataDownloaded) {
          // First time: data has been downloaded but not rendered
          this.dataDownloaded = true
        } else {
          // Second time: data has been downloaded and rendered
          this.mapLoading(false)
        }
      }
    },
    onError(e) {
      this.throwError(e)
      this.mapLoading(false)
      this.dataDownloaded = true
    },
    ...mapMutations(['mapLoading']),
    ...mapActions(['setZone', 'throwError'])
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
