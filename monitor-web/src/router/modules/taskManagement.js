import Layout from '../../layout/index.vue'

export default [
  {
    path: '/taskManagement',
    component:Layout,
    // redirect:'/taskManagement/index',
    children:[
      {
        path:'',
        name:'TaskManagement',
        meta:{
          title:'TaskManagement'
        },
        component: () => import('@/views/taskManagement/index.vue'),
      },
      {
        path:'TaskDetail',
        name:'TaskDetail',
        meta:{
          title:'TaskDetail'
        },
        component: () => import('@/views/taskManagement/detail.vue'),
      },
    ],
  },

]
