import Layout from '../../layout/index.vue'

export default [
  {
    path: '/taskManagement',
    component:Layout,
    // redirect:'/taskManagement/index',
    children:[
      {
        path:'',
        name:'taskManagement',
        meta:{
          title:'任务管理'
        },
        component: () => import('@/views/taskManagement/index.vue'),
      },
      {
        path:'taskDetail',
        name:'taskDetail',
        meta:{
          title:'任务详情'
        },
        component: () => import('@/views/taskManagement/detail.vue'),
      },
    ],   
  },

]