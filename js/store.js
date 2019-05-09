import Vue from 'vue'
import Vuex from 'vuex'

import {isodate} from './helpers'

Vue.use(Vuex)

const debug = process.env.NODE_ENV !== 'production'
const emitter = new Vue()

async function getData(path) {
    const response = await fetch(`/${path}`)
    if (!response.ok) {
      let msg = response.statusText
      try {
        const data = await response.json()
        msg = data.message || data.msg || data.text || msg
      } finally {
        throw Error(msg)
      }
    }
    return await response.json()
}

const state = {
  date: new Date(),
  errors: [],
  error: undefined,
  geojson: undefined,
  homepage: 'https://github.com/etalab/geozones',
  level: undefined,
  levels: [],
  dataLoading: false,
  mapLoading: true,
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
  loading: state => state.mapLoading || state.dataLoading
};

const mutations = {
  date(state, date) {
    state.date = date
  },
  error(state, error) {
    state.errors.push(error)
    state.error = error
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
  mapLoading(state, value) {
    state.mapLoading = value
  },
  dataLoading(state, value) {
    state.dataLoading = value
  },
  showNav(state, value) {
    state.showNav = value
  },
  zone(state, value) {
    state.zone = value
  },
}

const actions = {
  async fetchLevels({ commit, dispatch }) {
    try {
      commit('dataLoading', true)
      const response = await getData('levels')
      commit('levels', response)
      commit('dataLoading', false)
    } catch (error) {
      await dispatch('throwError', error)
    }
  },
  async setDate({ commit }, date) {
    commit('date', date)
  },
  async setLevel({ commit }, level) {
    commit('level', level)
  },
  async setZone({ commit, dispatch }, zone) {
    if (zone.hasOwnProperty('properties')) {
      // It's a geojson object
      commit('zone', zone)
    } else {
      // It's a GeoID
      commit('dataLoading', true)
      try {
        const response = await getData(`zones/${zone}`)
        commit('zone', response)
      } catch(error) {
        await dispatch('throwError', error)
      } finally {
        commit('dataLoading', false)
      }
    }
  },
  async throwError({ commit }, error) {
    let msg = 'An unknown error occured'
    switch (typeof error) {
      case 'string':
        msg = error
        console.error(msg)
        break
      case 'object':
        msg = error.message || error.msg || error.hasOwnProperty('toString') ? error.toString() : msg
        console.error(error)
        break
      default:
        console.error(error)
    }
    commit('error', msg)
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
