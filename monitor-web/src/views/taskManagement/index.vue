<template>
  <div class="box-card">
    <el-card shadow="none">
      <div class="form-create-header">
        <div class="form-search">
          <el-input placeholder="Task Name" v-model="query.workName" size="small" style="width:200px;"></el-input>
          <el-button class="el-icon-search" @click="getWorkList()" size="small" type="primary"> Search</el-button>
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
                <span v-if="scope.row[info.key]==='all'">Full</span>
                  <span v-if="scope.row[info.key]==='realTime'">RealTime</span>
                 </span>

              <span v-else-if="info.key==='startTime'">
                                {{ $dateZoneFtt(scope.row[info.key]) }}
                            </span>
              <span v-else-if="info.key==='endTime'">
                                {{ $dateZoneFtt(scope.row[info.key]) }}
                            </span>
              <span v-else-if="info.key==='status'">
                                <el-tag :type="scope.row[info.key]?'success':'danger'">
                                        {{ scope.row[info.key] ? 'Running' : 'Closed' }}
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
          label: 'Task Name',
          key: 'workName',
        },
        {
          label: 'Sync Mode',
          key: 'syncMode',
        },
        {
          label: 'Creation Time',
          key: 'startTime',
        },
        {
          label: 'End Time',
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
      // Task list
      getWorkList(this.query).then(res => {
        // console.log(res);
        this.formList = res.data
      })
    },
    // Task detail page
    taskDetail(workName) {
      window.localStorage.setItem('workName', workName)
      this.$router.push(
          {
            name: 'TaskDetail',
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
