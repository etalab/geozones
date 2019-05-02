import 'buefy/dist/buefy.css'

import Vue from 'vue'
import App from './App.vue'

import store from './store'
import helpers from './helpers'
import Buefy from 'buefy'

import { library } from "@fortawesome/fontawesome-svg-core";
import { fas } from "@fortawesome/free-solid-svg-icons";
import { fab } from "@fortawesome/free-brands-svg-icons";
// internal icons
// import { faCheck, faInfoCircle, faExclamationTriangle, faExclamationCirle,
//     faArrowUp, faAngleRight, faAngleLeft, faAngleDown,
//     faEye, faEyeSlash, faCaretDown, faCaretUp } from "@fortawesome/free-solid-svg-icons";

import { FontAwesomeIcon } from "@fortawesome/vue-fontawesome"


library.add(fas, fab);
Vue.component("vue-fontawesome", FontAwesomeIcon);
Vue.use(Buefy, {
  defaultIconPack: 'fas',
  defaultIconComponent: 'vue-fontawesome'
});

Vue.config.devtools = true

Vue.filter('isodate', helpers.isodate)

new Vue({
  el: '#app',
  store,
  render: h => h(App),
})
