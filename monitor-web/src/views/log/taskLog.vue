<template>
  <div>
    <el-form :inline="true" :model="query" class="demo-form-inline">
<!--      <el-row>-->
<!--        <el-col :span="5.5">-->


          <el-form-item label="Content">
            <el-input size="small" clearable v-model="query.info" placeholder="Content"></el-input>
          </el-form-item>
<!--        </el-col>-->

<!--        <el-col :span="5.5">-->
          <el-form-item label="Type">
            <el-select size="small" clearable v-model="query.type" placeholder="Select">
              <el-option
                  v-for="item in queryTypeOptions"
                  :key="item.value"
                  :label="item.label"
                  :value="item.value">
              </el-option>
            </el-select>
          </el-form-item>
<!--        </el-col>-->

<!--        <el-col :span="8.5">-->
          <el-form-item label="Date">
            <el-date-picker size="small"
                            :picker-options="pickerOptions"
                            v-model="dateTimeRange"
                            type="datetimerange"
                            range-separator="to"
                            start-placeholder="Start Date"
                            end-placeholder="End Date">
            </el-date-picker>
          </el-form-item>
<!--        </el-col>-->
<!--        <el-col :span="5">-->
      <el-form-item >
        <el-button class="el-icon-search"  size="small" type="primary" @click="pre()">Pre</el-button>
        <el-button class="el-icon-search" size="small" type="primary" @click="searchTaskLog();">Search</el-button>
        <el-button class="el-icon-search" size="small" type="primary" @click="next()">Next</el-button>
      </el-form-item>
<!--        </el-col>-->
<!--      </el-row>-->
    </el-form>

    <div>
      <el-table
          v-loading="loading"
          :data="logList"
          style="width: 100%">
        <el-table-column
            prop="createTime"
            label="Date"
            sortable
            width="180">
          <template slot-scope="scope">
            {{ $dateZoneFtt(scope.row.time) }}
          </template>
        </el-table-column>

        <el-table-column
            prop="type"
            label="Type"
            width="180">

          <template slot-scope="scope">
            <el-button size="mini" v-if="scope.row.type==='INFO'" type="success" plain>{{ scope.row.type }}</el-button>
            <el-button size="mini" v-else-if="scope.row.type==='WARN'" type="warning" plain>{{ scope.row.type }}</el-button>
            <el-button size="mini" v-else-if="scope.row.type==='ERROR'" type="danger" plain>{{ scope.row.type }}</el-button>
            <el-button size="mini" v-else-if="scope.row.type==='TRACE'" type="primary" plain>{{ scope.row.type }}</el-button>
            <el-button v-else size="mini" type="primary" plain>{{ scope.row.type }}</el-button>
          </template>

        </el-table-column>
        <el-table-column
            prop="info"
            label="Content">
        </el-table-column>
      </el-table>
    </div>
  </div>
</template>


<script>
import {findLog} from '@/api/logManagement';

export default {


  data() {
    return {
      pickerOptions: {
        shortcuts: [
          {
            text: 'Last day',
            onClick(picker) {
              const end = new Date();
              const start = new Date();
              start.setTime(start.getTime() - 3600 * 1000 * 24);
              picker.$emit('pick', [start, end]);
            }
          }, {
            text: 'Last week',
            onClick(picker) {
              const end = new Date();
              const start = new Date();
              start.setTime(start.getTime() - 3600 * 1000 * 24 * 7);
              picker.$emit('pick', [start, end]);
            }
          }, {
            text: 'Last month',
            onClick(picker) {
              const end = new Date();
              const start = new Date();
              start.setTime(start.getTime() - 3600 * 1000 * 24 * 30);
              picker.$emit('pick', [start, end]);
            }
          }]
      },
      queryTypeOptions: [
        {label: "INFO", value: "INFO"},
        {label: "WARN", value: "WARN"},
        {label: "TRACE", value: "TRACE"},
        {label: "ERROR", value: "ERROR"},
      ],
      dateTimeRange: [new Date((new Date().getTime()) - (3600 * 1000)), new Date((new Date().getTime()) + (60 * 1000))],
      loading: false,
      query: {
        type: '',
        startTime: '',
        endTime: '',
        info: '',
        pageIndex: 0,
        pageSize: 10,
      },
      logList: [],
      total: Number.MAX_VALUE,
    }
  },


  mounted() {
    this.searchTaskLog(this.query.pageIndex)
  },

  methods: {
    pre() {
      this.query.pageIndex--;
      if(this.query.pageIndex<0){
        this.query.pageIndex=0;
      }
      this.findLog();
    },
    next() {
      this.query.pageIndex++;
      this.findLog();
    },
    searchTaskLog() {
      this.query.pageIndex = 0;
      this.findLog();
    },
    findLog() {
      this.query.startTime = new Date(this.dateTimeRange[0]).getTime(),
          this.query.endTime = new Date(this.dateTimeRange[1]).getTime(),
          this.loading = true
      findLog(this.query).then(res => {
        this.logList = res.data
        this.loading = false
      });
    }

  }
}
</script>
