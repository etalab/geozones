import Vue from 'vue'
import Vuex from 'vuex'

import {isodate} from './helpers'

Vue.use(Vuex)

const debug = process.env.NODE_ENV !== 'production'

async function getData(path) {
    const response = await fetch(`/${path}`)
    return await response.json()
}

const state = {
  date: new Date(),
  geojson: undefined,
  homepage: 'https://github.com/etalab/geozones',
  level: undefined,
  levels: [],
  loading: false,
  mapConfig: {
    center: [2.4, 42],
    style: 'mapbox://styles/mapbox/light-v9',
    zoom: 4,
    token: 'pk.eyJ1Ijoibm9pcmJpemFycmUiLCJhIjoiY2o4b3IzNzdpMDZuMDMxcGNrZnIycHJ4aCJ9.NfhQARV5xOKTiinl4nb29g',
  },
  showNav: false,
  zone: undefined,
}

const getters = {
  date: state => state.date,
  geojson: state => state.geojson,
  github: state => state.homepage,
  homepage: state => state.homepage,
  level: state => state.level,
  levels: state => state.levels,
  levelUrl: state => state.level ? `/levels/${state.level.id}@${isodate(state.date)}` : undefined,
  loading: state => state.loading,
  mapConfig: state => state.mapConfig,
  showNav: state => state.showNav,
  zone: state => state.zone,
};

const mutations = {
  date(state, date) {
    state.date = date
  },
  geojson(state, value) {
    state.geojson = value
  },
  level(state, value) {
    state.level = value
  },
  levels(state, value) {
    state.levels = value
  },
  loading(state, value) {
    state.loading = value
  },
  showNav(state, value) {
    state.showNav = value
  },
  zone(state, value) {
    state.zone = value
  },
}

const actions = {
  async fetchLevels({ commit }) {
    try {
      commit('loading', true)
      const response = await getData('levels')
      commit('levels', response)
      commit('loading', false)
    } catch (error) {
      console.error(error)
    }
  },
  async setDate({ commit }, date) {
    commit('date', date)
  },
  async setLevel({ commit }, level) {
    commit('level', level)
  },
  async setZone({ commit }, zone) {
    if (zone.hasOwnProperty('properties')) {
      // It's a geojson object
      commit('zone', zone)
    } else {
      // It's a GeoID
      commit('loading', true)
      const response = await getData(`zones/${zone}`)
      commit('zone', response)
      commit('loading', false)
    }
  },
  async toggleNav({commit, getters}) {
    commit('showNav', !getters.showNav)
  },
}

export default new Vuex.Store({
    actions,
    getters,
    mutations,
    state,
})
