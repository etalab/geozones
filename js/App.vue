<template>
<main>
  <world-map></world-map>
  <aside id="layer-picker" class="panel">
    <layer-picker></layer-picker>
  </aside>
  <aside id="zone-details" class="panel" v-if="zone">
    <zone-details></zone-details>
  </aside>
</main>
</template>

<script>
import {mapGetters, mapActions} from 'vuex'
import LayerPicker from './LayerPicker.vue'
import WorldMap from './WorldMap.vue'
import ZoneDetails from './ZoneDetails.vue'


export default {
  components: {WorldMap, LayerPicker, ZoneDetails},
  created() {
    this.fetchLevels()
  },
  computed: {
    ...mapGetters(['zone'])
  },
  methods: {
    ...mapActions(['fetchLevels'])
  }
}
</script>

<style scoped lang="postcss">
main {
  height: 100%;
}

#zone-details {
  position: absolute;
  z-index: 1;
  top: 10px;
  right: 10px;
}

#layer-picker {
  position: absolute;
  z-index: 1;
  top: 10px;
  left: 10px;
}

.panel {
  padding: 10px;
  background-color: white;
  border-radius: 2px;
}

</style>
