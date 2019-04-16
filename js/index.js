import Vue from 'vue'
import App from './App.vue'

import store from './store'


// import fontawesome from '@fortawesome/fontawesome'
// import { faSpinner, faStar, faCheck, faTimes } from '@fortawesome/fontawesome-free-solid'

// fontawesome.library.add(faSpinner, faStar, faCheck, faTimes)

Vue.config.devtools = true

new Vue({
  el: '#app',
  store,
  render: h => h(App),
})
