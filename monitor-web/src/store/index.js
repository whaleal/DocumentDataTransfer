import Vue from 'vue'
import Vuex from 'vuex'

import tagsView from './modules/tagsView'

// import settings from './modules/settings'
import getters from './getters'

Vue.use(Vuex)

const store = new Vuex.Store({
  modules: {

    tagsView,
    // settings
  },
  getters
})

export default store
