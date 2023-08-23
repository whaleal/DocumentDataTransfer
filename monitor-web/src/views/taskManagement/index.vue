<template>
  <div class="box-card">
    <el-card shadow="none">
      <div class="form-create-header">
        <div class="form-search">
          <el-input placeholder="任务名" v-model="query.workName" size="small" style="width:200px;"></el-input>
          <el-button class="el-icon-search" @click="getWorkList()" size="small" type="primary"> 搜索</el-button>
        </div>
      </div>

      <div>
        <el-table
            :data="formList"
            style="width:100%"
            :row-style="{height:'60px'}"
            border
        >
          <el-table-column
              v-for="info in tableHeader" :key='info.key'
              :property='info.key'
              :label='info.label'
              show-overflow-tooltip
              align="center"
          >
            <template slot-scope="scope">
                            <span v-if="info.key==='workName'" @click="taskDetail(scope.row.workName)" class="hover">
                                {{ scope.row[info.key] }}
                            </span>

              <span v-else-if="info.key==='syncMode'">
                <span v-if="scope.row[info.key]==='all'">全量</span>
                              <span v-if="scope.row[info.key]==='realTime'">实时</span>
                            </span>
              <span v-else-if="info.key==='startTime'">
                                {{ $dateZoneFtt(scope.row[info.key]) }}
                            </span>
              <span v-else-if="info.key==='endTime'">
                                {{ $dateZoneFtt(scope.row[info.key]) }}
                            </span>
              <span v-else-if="info.key==='status'">
                                <el-tag :type="scope.row[info.key]?'success':'danger'">
                                        {{ scope.row[info.key] ? '运行' : '关闭' }}
                                </el-tag>
                            </span>
              <span v-else>{{ scope.row[info.key] }} </span>
            </template>
          </el-table-column>


        </el-table>
      </div>

    </el-card>
  </div>
</template>

<script>

import {getWorkList} from '@/api/taskManagement';

export default {

  data() {
    return {
      query: {
        workName: '',
      },
      total: 0,
      formList: [],
      tableHeader: [
        {
          label: '任务名',
          key: 'workName',
        },
        {
          label: '同步模式',
          key: 'syncMode',
        },
        {
          label: '创建时间',
          key: 'startTime',
        },
        {
          label: '结束时间',
          key: 'endTime',
        }
      ]
    }
  },

  mounted() {
    this.getWorkList()
  },

  methods: {
    getWorkList() {
      //任务列表
      getWorkList(this.query).then(res => {
        // console.log(res);
        this.formList = res.data
      })
    },
    //任务详情页
    taskDetail(workName) {
      window.localStorage.setItem('workName', workName)
      this.$router.push(
          {
            name: 'taskDetail',
            query: {
              workName: workName
            }
          }
      )
    },
  }
}
</script>

<style>

.form-create-header {
  margin: 10px 0;
}


</style>
