<template>
  <div>
    <div>
      <div class="block">
        <el-date-picker
            v-model="dateTimeRange"
            type="datetimerange"
            :picker-options="pickerOptions"
            range-separator="至"
            start-placeholder="开始日期"
            end-placeholder="结束日期"
            align="right">
        </el-date-picker>
        <el-button size="large" type="primary" @click="getWorkMonitor()">更新监控</el-button>
      </div>

    </div>

    <!-- 实时监控数据-->
    <el-divider></el-divider>
    <div class="main-container" v-loading="loading">
      <el-row :gutter="10" class="totalEchart">
        <el-col :span="24" v-for="(item,index) in monitorDataList" :key="index" v-loading="loading">
          <el-col :span="24" v-for="(it,indexs) in dataTypeList" :key="indexs">
            <div v-if="it ===item.name&&item.isShow">
              <task-echarts :monitorData='item.monitorData' :createTime="item.createTime" :message="item.message"
                            :name="item.name" :info='item.info' :unit="item.unit"></task-echarts>
            </div>
          </el-col>
        </el-col>
      </el-row>
    </div>
  </div>
</template>

<script>
import {getWorkMonitor} from '@/api/taskManagement.js'
import taskEcharts from '@/components/taskEcharts.vue'

export default {
  components: {
    taskEcharts
  },
  props: {
    workName: {
      type: String,
      default: () => {
      }
    }
  },

  data() {
    return {
      dateTimeRange: [new Date((new Date().getTime()) - (6 * 3600 * 1000)), new Date((new Date().getTime()) + (60 * 1000))],

      pickerOptions: {
        shortcuts: [{
          text: '最近一周',
          onClick(picker) {
            const end = new Date();
            const start = new Date();
            start.setTime(start.getTime() - 3600 * 1000 * 24 * 7);
            picker.$emit('pick', [start, end]);
          }
        }, {
          text: '最近一个月',
          onClick(picker) {
            const end = new Date();
            const start = new Date();
            start.setTime(start.getTime() - 3600 * 1000 * 24 * 30);
            picker.$emit('pick', [start, end]);
          }
        }, {
          text: '最近三个月',
          onClick(picker) {
            const end = new Date();
            const start = new Date();
            start.setTime(start.getTime() - 3600 * 1000 * 24 * 90);
            picker.$emit('pick', [start, end]);
          }
        }]
      },
      query: {
        startTime: 0,
        endTime: new Date().getTime(),
        type: ''
      },

      dataTypeList: [
        // 全量的
        'fullCount',
        'fullThreadNum',
        'fullRate',
        'fullCache',
        // 实时
        "realTimeRate",
        "realTimeCache",
        "realTimeThreadNum",
        "realTimeExecute",
        "realTimeDelayTime",
        "realTimeIncProgress",
        // 主机的
        'hostCPU',
        'hostMemory',
        'netIO',
        'status',
      ],
      loading: false,
      monitorDataList: [],
      isShow: false

    }
  },
  watch: {},
  created() {

    this.getWorkMonitor()
  },
  mounted() {

  },

  methods: {
    //获取监控数据
    getWorkMonitor() {
      this.query.startTime = this.dateTimeRange[0].getTime();
      this.query.endTime = this.dateTimeRange[1].getTime();
      //hostUsage
      this.monitorDataList = []
      this.dataTypeList.forEach((item, index) => {
        this.monitorDataList.push({})
        this.query.type = item

        getWorkMonitor(this.workName, this.query).then(res => {
          this.monitorDataList.splice(index, 1, {
            monitorData: res.data,
            createTime: res.createTime,
            message: res.message,
            name: res.name,
            unit: res.unit,
            isShow: res.isShow
          })
        })
      })

    },

  },

}
</script>
