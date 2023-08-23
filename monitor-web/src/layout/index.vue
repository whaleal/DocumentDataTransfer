<template>
    <div>
        <el-container>
            <Aside :isCollapse='isCollapse' />  
            <el-container>       
                <el-header style="padding-left:0;">
                    <Header></Header>
                </el-header>   
                <el-container>
                    <el-main>
                        <!-- <div class="bread-crumb" > -->
                            <tags-view />
                        <!-- </div> -->
                        <div class="main-body">
                            <transition mode="out-in">
                                <router-view>
                                </router-view>
                            </transition>
                        </div>
                        <el-footer><lay-footer></lay-footer></el-footer>
                    </el-main>
                </el-container>
            </el-container>
        </el-container>
    </div>
</template>

<script>
import Aside from './components/aside.vue'
import Header from './components/header.vue'
import  tagsView  from './components/tagsView.vue'
import LayFooter from './components/layFooter.vue'
import bus from '@/common/bus'
export default {
    name:'layout',
    components:{
        Aside,
        Header,
        tagsView,
        LayFooter
    },
    data(){
        return{
            isCollapse:false
        }
    },

    watch:{
       
    },

    mounted(){
       	// 通过 Event Bus 进行组件间通信，来取消通知红点
		bus.$on('collapse',val=>{
			this.isCollapse = val
		})
    },

    methods:{
       
    }
}
</script>

<style >
.bread-crumb {
	height: 40px;
	line-height: 38px;
	background: #fff;
    box-shadow: 0 5px 10px #ddd;
    display: flex;
    align-items: center;
}

.el-header {
	color: #333;
    border-bottom: 1px solid rgb(231, 238, 236);
	text-align: center;
}

.el-main {
	background-color: #e9eef3;
    height: calc(100vh - 60px);
	padding: 0 !important;
}

.main-body{
    height: calc(100vh - 160px);
}

.el-footer{
    background-color: #B3C0D1;
    color: #333;
    height: auto !important;
    text-align: center;
    line-height: 50px;
}
</style>

<style>



/* 路由的简单过渡动画效果 */
.v-enter {
	opacity: 0;
}
.v-enter-to {
	opacity: 1;
}
.v-enter-active {
	transition: 0.3s;
}
.v-leave {
	opacity: 1;
}
.v-leave-to {
	opacity: 0;
}
.v-leave-active {
	transition: 0.3s;
}
</style>