<template>
<main>
  <b-loading :active="loading"></b-loading>
  <navbar></navbar>
  <world-map></world-map>
  <aside class="right-sidebar" v-if="zone">
    <zone-details></zone-details>
  </aside>
</main>
</template>

<script>
import {mapGetters, mapActions, mapState} from 'vuex'
import Navbar from './Navbar.vue'
import WorldMap from './WorldMap.vue'
import ZoneDetails from './ZoneDetails.vue'


export default {
  components: {WorldMap, ZoneDetails, Navbar},
  created() {
    this.fetchLevels()
  },
  computed: {
    ...mapState(['zone', 'error']),
    ...mapGetters(['loading'])
  },
  methods: {
    ...mapActions(['fetchLevels'])
  },
  watch: {
    error(error) {
      this.$toast.open({
          duration: 5000,
          message: error,
          position: 'is-bottom',
          type: 'is-danger'
      })
    }
  }
}
</script>

<style scoped lang="postcss">
$navbar-height: 3.25rem;

main {
  height: 100vh;

  nav.navbar {
    height: $navbar-height;
  }
}

.right-sidebar {
  position: absolute;
  z-index: 1;
  top: calc($navbar-height + 10px);
  right: 10px;
  min-width: 350px; /* TODO: Make it responsive */
  max-height: 99%;
  opacity: 0.9;
}
</style>
