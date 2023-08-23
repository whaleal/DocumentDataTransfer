import Layout from '../../layout/index.vue'

export default [
  {
    path: '/form',
    component:Layout,
    children:[
      {
        path:'',
        name:'form',
        meta:{
          title:'配置管理'
        },
        component: () => import('@/views/form/workList.vue'),
      },
    ],
  },

];
