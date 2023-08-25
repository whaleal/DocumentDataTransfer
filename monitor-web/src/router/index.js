import Vue from 'vue'
import Router from 'vue-router'
import Layout from '../layout/index.vue'


import taskManagement from './modules/taskManagement'

Vue.use(Router)

export const routers = [
    // {
    //   path: '/',
    //   redirect: '/home',
    // },

    //侧边栏 首页 路由地址
    {
        path: "/",
        // name: "Layout",
        component: Layout,
        redirect: 'home',
        children: [
            {
                path: "home",
                name: "home",
                meta: {
                    title: '首页',
                    noCache: true,
                    affix: true
                },
                component: () => import("@/views/home/index.vue")
            },

        ]
    },
    ...taskManagement,
    {
        path: '/log',
        component: Layout,
        // redirect:'/taskManagement/index',
        children: [
            {
                path: '',
                name: 'logList',
                meta: {
                    title: '日志列表'
                },
                component: () => import('@/views/log/taskLog.vue'),
            }
        ],
    },
]


export default new Router({
    // mode: 'history',
    base: import.meta.env.BASE_URL,
    scrollBehavior: () => ({y: 0}),
    routes: routers
})
