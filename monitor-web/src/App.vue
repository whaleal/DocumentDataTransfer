<template>
  <div id="app">
    <keep-alive>
      <div class="xCutScreen">
        <router-view v-if="isRouterAlive"/>
      </div>
    </keep-alive>
  </div>
</template>

<script>
export default {
  name: 'App',
  data(){
    return{
      isRouterAlive:true,
    }
  },
  provide(){
    return {
      reload:this.reload
    }
  },
  methods:{
    reload(){
       this.isRouterAlive = false
          this.$nextTick( function () {
              this.isRouterAlive = true
          })
    }
  }
}
</script>

<style>
html,body{
  margin: 0;
  padding: 0;
}
.xCutScreen{
  height: 100vh;
  width: 100vw;
  overflow: hidden;
}


</style>
