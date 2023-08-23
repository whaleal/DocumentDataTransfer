<template>
  <div>
    <div class="monitor_card">
      <div class="mongo_monitor-title">
        <div class="monitor-tips">
          <h5>{{ name.toUpperCase() }}</h5>
          <div>
            <el-popover
                placement="bottom-start"
                width="200"
                trigger="hover">
              <ul>
                <li v-for="(it,key,index) in message" :key='index'>
                  <span style="font-weight:bold;">{{ key }}</span>：<span>{{ it }}</span>
                </li>
              </ul>
              <v-mdi name='mdi-helpCircleOutline' class="help-tips" slot="reference"></v-mdi>

            </el-popover>
          </div>
        </div>

      </div>
      <div style="height:250px;flex:1;">
        <div :id="name" style="height:100%;" shadow="none" class="monitor_echart">

        </div>
      </div>
    </div>
    <el-divider></el-divider>
    <!-- </el-card> -->
  </div>
</template>

<script>
export default {
  props: {
    monitorData: {
      type: Object,
      default: () => {
      }
    },
    createTime: {
      type: Array,
      default: () => {
      }
    },
    timeGranularity: {
      type: Number,
      default: () => {
      }
    },
    message: {
      type: Object,
      default: () => {
      }
    },
    name: {
      type: String,
      default: () => {
      }
    },
    info: {
      type: Object,
      default: () => {
      }
    },
    hostId: {
      type: String,
      default: () => {
      }
    },
    type: {
      type: String,
      default: null
    },
    unit: {
      type: String,
      default: null
    }
  },
  data() {
    return {
      monitorList: this.monitorData,
      disk: '',
      echartName: this.name,
      diskIO: '',
      xAxis: '',
      infoList: {},
      unitName: ''
    }
  },
  created() {
    this.unitName = this.unit
    this.xAxis = this.createTime
    this.$nextTick(() => {
      this.drawer();
    })
  },
  mounted() {


  },

  watch: {},

  methods: {
    drawer() {


      this.series = [];
      for (let key in this.monitorList) {
        //    if(this.type == this.name){
        this.series.push(
            {
              name: key,
              type: 'line',
              smooth: true,
              symbol: 'none',
              data: this.monitorList[key],

            }
        )
      }

      let data = this.series
      var option = {}
      setTimeout(() => {
        if (data[0].data.length == 0) {
          option = {
            title: {
              text: '暂无数据',
              x: 'center',
              y: 'center',
              textStyle: {
                fontSize: 14,
                fontWeight: 'normal'
              }
            }
          }
        } else {
          option = {
            title: {

              textStyle: {}
            },

            grid: {
              // x:'12%',
              y: '20%',
              y2: '20%',
              left: '15%',
              // height:'90%'
            },

            legend: {
              show: true
            },
            dataZoom: [
              {
                // type: 'inside',
                show: false,
                xAxisIndex: 0,
                zoomOnMouseWheel: false //鼠标滚轮不能触发缩放
              }
            ],
            toolbox: {
              feature: {
                // dataView: { show: true, readOnly: false },
                // magicType: { show: true, type: ['line', 'bar'] },
                // restore: { show: true },
                // saveAsImage: { show: true }
                dataZoom: {
                  show: true,
                  filterMode: 'filter',
                  // title: {
                  //     zoom: '缩放',
                  //     back: '还原'
                  // },
                  iconStyle: { //不需要图标可以设置隐藏按钮
                    opacity: 0
                  },
                  //缩放和还原的图标路径，不指定则显示默认图标
                  // icon: {
                  //     zoom: '',
                  //     back: ''
                  // },
                  // xAxisIndex: true, //指定哪些X轴可以被控制
                  yAxisIndex: 'none' //指定哪些Y轴可以被控制（设置为 false，则不控制任何y轴）
                },
                // restore: {} //区域缩放重置
              },
            },
            tooltip: {
              trigger: 'axis',
              axisPointer: {
                // type:'cross',
                snap: true,
                lineStyle: {
                  type: 'solid',
                  width: 2
                }
              },

              confine: true,

            },
            xAxis: {
              type: 'category',
              data: this.$dateListZoneFtt(this.xAxis),
              // offset:10,
              boundaryGap: false,
              axisLabel: {
                // interval: this.$dateListZoneFtt(this.xAxis) > 15 ? 30 : 0,   //主要设置其间隔，间隔为3
              }
            },

            yAxis: {
              type: 'value',
              minInterval: 1,
              name: this.unitName,
              nameTextStyle: {
                color: "#aaa",
                nameLocation: "start",
              },
            },

            series: data
          }
        }
        let myCharts = document.getElementById(this.name);
        let myChart = this.$echarts.init(myCharts);
        myChart.setOption(option, true);
        myChart.dispatchAction({
          // 默认选中区域缩放
          type: 'takeGlobalCursor',
          key: 'dataZoomSelect',
          dataZoomSelectActive: true
        });
        myChart.group = 'group1';
        this.$echarts.connect('group1');

        this.series = [];
        window.addEventListener('resize', () => {
          myChart && myChart.resize()
        })

      }, 0)


      // }


    },





  }
}
</script>

<style>

.monitor_card {
  /* display: flex ; */

}


.mongo_monitor-title {
  padding: 0 10px 10px;
  width: 400px;
  vertical-align: top;
  color: #888;
}


.help-tips {
  margin-left: 5px;
  width: 18px;
  height: 18px;
}

.monitor-tips {
  display: flex;
}

.monitor_echart {
  padding-top: 30px;
}

.mongo_detail_data {
  margin-top: 25px;
}

.detail_data span {
  font-size: 8px;
}

.data_list {
  margin-bottom: 10px;
}

.data_list span {
  font-size: 8px;
}


/* .memo{
    flex: 1;
} */

</style>
