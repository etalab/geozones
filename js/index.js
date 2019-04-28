// import 'v-calendar/lib/v-calendar.min.css'

import 'buefy/dist/buefy.css'

import Vue from 'vue'
// import VCalendar from 'v-calendar'
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


// import fontawesome from '@fortawesome/fontawesome'
// import { faSpinner, faStar, faCheck, faTimes } from '@fortawesome/fontawesome-free-solid'

// fontawesome.library.add(faSpinner, faStar, faCheck, faTimes)


// Use v-calendar, v-date-picker & v-popover components
// Vue.use(VCalendar, {
//   // firstDayOfWeek: 2,  // Monday
//   // ...,                // ...other defaults
// });

Vue.config.devtools = true

Vue.filter('isodate', helpers.isodate)

new Vue({
  el: '#app',
  store,
  render: h => h(App),
})
