<template>
  <div class="box-card">
    <el-card shadow="none">
      <el-page-header @back="goBack" :content="taskName" style="padding-bottom:10px;">
      </el-page-header>
      <el-tabs v-model="activeName" @tab-click="handleClick">
        <el-tab-pane label="基本信息" name='first'>
          <basic-info :workName='workName' v-if="activeName == 'first' "></basic-info>
        </el-tab-pane>
        <el-tab-pane label="监控" name="second">
          <monitor :workName='workName' v-if="activeName == 'second' "></monitor>
        </el-tab-pane>
<!--        <el-tab-pane label="日志" name="third">-->
<!--          <task-log :workName='workName' v-if="activeName == 'third'"></task-log>-->
<!--        </el-tab-pane>-->

      </el-tabs>
    </el-card>
  </div>
</template>

<script>
import basicInfo from './basicInfo.vue'
import taskLog from './taskLog.vue';
import monitor from './monitor.vue'

export default {
  components: {
    basicInfo,
    taskLog,
    monitor
  },

  data() {
    return {
      taskName: '',
      workName: '',
      activeName: 'first',
    }
  },
  created() {
    this.workName = localStorage.getItem('workName');
  },

  mounted() {
    this.taskName = this.$route.query.workName;
  },

  activated() {
  },

  methods: {
    handleClick(data) {
      this.activeName = data.name
    },

    //返回上一页
    goBack() {
      this.$router.go(-1)
    }
  }


}
</script>

<style>
.el-tabs__nav-wrap::after {
  background-color: #fff !important;
}
</style>
