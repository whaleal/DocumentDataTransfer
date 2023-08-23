import Vue from 'vue'

import App from './App.vue'
import router from './router'
import store from './store'
import ElementUI from 'element-ui';
import 'element-ui/lib/theme-chalk/index.css';
import './assets/main.css'
// 样式重置
import './assets/resetCss/reset.css'
Vue.use(ElementUI);

import VueMdijs from 'vue-mdijs'
// import VueCron from 'vue-cron'
import { mdiHelpCircleOutline } from '@mdi/js'

VueMdijs.add({ mdiHelpCircleOutline})
Vue.use(VueMdijs)
// Vue.use(VueCron)

//echarts
import * as echarts from "echarts"
Vue.prototype.$echarts = echarts;

// import ECharts from 'vue-echarts'
import { use } from 'echarts/core'

import {
  CanvasRenderer
} from 'echarts/renderers'
import {
  BarChart
} from 'echarts/charts'
import {
  GridComponent,
  TooltipComponent
} from 'echarts/components'

use([
  CanvasRenderer,
  BarChart,
  GridComponent,
  TooltipComponent
]);


/**************************************时间格式化处理************************************/
Vue.prototype.$dateFtt = function dateFtt(date) {
  try {
      let fmt = "yyyy-MM-dd hh:mm:ss";
      var o = {
          "M+": date.getMonth() + 1,     //月份
          "d+": date.getDate(),     //日
          "h+": date.getHours(),     //小时
          "m+": date.getMinutes(),     //分
          "s+": date.getSeconds(),     //秒
          "q+": Math.floor((date.getMonth() + 3) / 3), //季度
          "S": date.getMilliseconds()    //毫秒
      };
      if (/(y+)/.test(fmt))
          fmt = fmt.replace(RegExp.$1, (date.getFullYear() + "").substr(4 - RegExp.$1.length));
      for (var k in o)
          if (new RegExp("(" + k + ")").test(fmt))
              fmt = fmt.replace(RegExp.$1, (RegExp.$1.length == 1) ? (o[k]) : (("00" + o[k]).substr(("" + o[k]).length)));
      return fmt;
  } catch (e) {
      console.log(e);
      return date;
  }
}
Vue.prototype.$dateZoneFtt = function dateZoneFtt(date, timezone) {
  // console.log(timestamp);
  // console.log(timezone);
  if(timezone==null){
      timezone=localStorage.getItem("timezone");
  }
  let timestamp=new Date(date).getTime();
  if (timezone === 'Asia/Shanghai') {
      timestamp= timestamp + 3600000 * 8;
  }
  else  if (timezone === 'Asia/Tokyo') {
      timestamp= timestamp + 3600000 * 9;
  }
  else  if (timezone === 'America/Phoenix') {
      timestamp= timestamp + 3600000 * 13;
  }
  else  if (timezone === 'Australia/Sydney') {
      timestamp= timestamp + 3600000 * 10;
  }

  // console.log(timestamp);
  // console.log(timezone);

  return this.$dateFtt(new Date(timestamp));
}

Vue.prototype.$dateListZoneFtt = function dateListZoneFtt(dateList, timezone) {

  // console.log(timezone);
  if(timezone==null){
      timezone=localStorage.getItem("timezone");
  }

//  if(timezone==null){
//       timezone='UTC+8';
//   }
  let dateListTemp=[];
  for(let index=0;index<dateList.length;index++){

    dateListTemp.push(this.$dateZoneFtt(dateList[index],timezone));

  }
  return dateListTemp;
}

/**************************************浮点型长度设置************************************/
Vue.prototype.$doubleFtt = function $doubleFtt(double, length) {
  return double.toFixed(length)
}

new Vue({
  router,
  store,
  render: (h) => h(App)
}).$mount('#app')
