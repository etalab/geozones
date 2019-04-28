import Vue from 'vue'
import Vuex from 'vuex'

Vue.use(Vuex)

const debug = process.env.NODE_ENV !== 'production'

async function getData(path) {
    const response = await fetch(`/${path}`)
    return await response.json()
}

const state = {
    loading: false,
    mapConfig: {
        center: [2.4, 42],
        style: 'mapbox://styles/mapbox/light-v9',
        zoom: 4,
        token: 'pk.eyJ1Ijoibm9pcmJpemFycmUiLCJhIjoiY2o4b3IzNzdpMDZuMDMxcGNrZnIycHJ4aCJ9.NfhQARV5xOKTiinl4nb29g',
    },
    levels: [],
    level: undefined,
    geojson: undefined,
    date: new Date(),
    zone: undefined,
    homepage: 'https://github.com/etalab/geozones',
}

const getters = {
  loading: state => state.loading,
  mapConfig: state => state.mapConfig,
  level: state => state.level,
  levelUrl: state => `/levels/${state.level.id}`,
  date: state => state.date,
  levels: state => state.levels,
  homepage: state => state.homepage,
  github: state => state.homepage,
  geojson: state => state.geojson,
  zone: state => state.zone,
//   currentDate: state => new Date(state.run.date).toLocaleDateString(),
//   currentQuery: state => state.query.query,
//   getDataset: state => id => state.datasets.find(row => row.id == id),
//   oembedApi: state => `${state.details.server}/api/1/oembed`,
  // oembedUrls: state => [
  //     `${state.domain}/datasets/*/`,
  //     `${state.domain}/*/datasets/*/`,
  //     `${state.domain}/reuses/*/`,
  //     `${state.domain}/*/reuses/*/`,
  // ],
};

const mutations = {
    loading(state, value) {
        state.loading = value
    },
    levels(state, value) {
        state.levels = value
    },
    level(state, value) {
        state.level = value
    },
    geojson(state, value) {
      state.geojson = value
    },
    date(state, date) {
      state.date = date
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
    async setLevel({ commit, dispatch }, level) {
        console.log('setLevel', level)
        commit('loading', true)
        commit('level', level)
        const response = await getData(`levels/${level.id}`)
        commit('geojson', response)
        commit('loading', false)
        // await dispatch('getToc')
    },
    async setDate({ commit, dispatch }, date) {
      commit('date', date)
      console.log('setDate', date)
      // await dispatch('getToc')
    },
    async setZone({ commit, dispatch }, zone) {
        console.log('setZone', zone)
        commit('zone', zone)
        console.log('setZone done')
        // await dispatch('getToc')
    },
}


export default new Vuex.Store({
    actions,
    getters,
    mutations,
    state,
})
