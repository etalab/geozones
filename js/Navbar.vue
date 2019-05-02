<template>
<nav class="navbar main-navbar is-transparent is-fixed-top">
  <div class="navbar-brand">
    <a class="navbar-item" :href="homepage">
      Geozones
    </a>
    <div class="navbar-burger burger" @click="toggleNav" :class="{'is-active' : showNav}">
      <span></span>
      <span></span>
      <span></span>
    </div>
  </div>

  <div class="navbar-menu" :class="{'is-active' : showNav}">
    <div class="navbar-start">
      <b-dropdown v-model="level" aria-role="menu" class="navbar-item">
        <b-button slot="trigger" icon-left="layer-group" icon-right="caret-down">
          {{ level ? level.label : 'No level selected' }}
        </b-button>
        <b-dropdown-item v-for="level in levels" :key="level.id" aria-role="listitem" :value="level">
          {{ level.label }}
        </b-dropdown-item>
      </b-dropdown>

      <b-dropdown aria-role="menu" class="calendar-dropdown navbar-item" ref="calendarDropdown">
        <b-button slot="trigger" icon-left="calendar-day" icon-right="caret-down">
          {{ date | isodate }}
        </b-button>
        <b-dropdown-item aria-role="menu-item" custom paddingless>
          <b-datepicker v-model="date" inline></b-datepicker>
        </b-dropdown-item>
      </b-dropdown>
    </div>

    <div class="navbar-end">
      <a class="navbar-item" :href="github" target="_blank">
        <b-icon pack="fab" icon="github"></b-icon>
      </a>
    </div>
  </div>
</nav>
</template>

<script>
import {mapGetters, mapActions} from 'vuex'

export default {
  computed: {
    level: {
      get() {
        return this.$store.state.level
      },
      set(level) {
        this.setLevel(level)
      }
    },
    date: {
      get() {
        return this.$store.state.date
      },
      set(date) {
        this.setDate(date)
        console.log('refs', this.$refs)
        this.$refs.calendarDropdown.toggle()
      }
    },
    ...mapGetters(['levels', 'homepage', 'github', 'showNav']),
  },
  methods: {
    ...mapActions(['setLevel', 'setDate', 'toggleNav'])
  },
}
</script>

<style lang="postcss">
.main-navbar {
  background-color: rgba(255, 255, 255, 0.6);

  .dropdown + .dropdown {
    margin-left: 0;
  }
}

.calendar-dropdown {
  .dropdown-content {
      padding: 0;
  }
}
</style>
