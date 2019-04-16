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
    zone: undefined,
}

const getters = {
  loading: state => state.loading,
  mapConfig: state => state.mapConfig,
  level: state => state.level,
  levels: state => state.levels,
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
        commit('level', level)
        console.log('setLevel', level)
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
