<template>
  <div>
    <el-descriptions :title="workName" :column="1" size="medium" border>
      <el-descriptions-item label="主机名">{{ info.config.hostName }}</el-descriptions-item>
      <el-descriptions-item label="进程ID">
        <el-tag size="small" style="margin-right:10px;">{{ info.config.pid }}</el-tag>
      </el-descriptions-item>
      <el-descriptions-item label="进程目录">{{ info.config.bootDirectory }}</el-descriptions-item>
      <el-descriptions-item label="当前同步模式">{{ info.config.syncMode }}</el-descriptions-item>
      <el-descriptions-item label="创建时间">{{ this.$dateZoneFtt(info.config.startTime, null) }}</el-descriptions-item>
      <el-descriptions-item label="是否限速" v-if="info.config.isLimit">
        <el-button size="mini" type="danger" round>限速</el-button>
      </el-descriptions-item>
    </el-descriptions>

    <div style="margin-top:10px;">

      <el-collapse v-model="activeName" accordion>
        <el-collapse-item title="配置" name="1">
          <el-form
              :model="info.config"
              ref="createOrEditFormRef"
              label-position="right">
            <el-row>
              <el-col :span="12">
                <el-form-item label="任务名" label-width="80px">
                  <el-input v-model="info.config.workName" size="small" disabled></el-input>
                </el-form-item>
              </el-col>
            </el-row>

            <el-form-item label="source URL" label-width="100px">
              <el-input v-model="info.config.sourceDsUrl" size="small" style="width:100%" disabled>
              </el-input>
            </el-form-item>
            <el-form-item label="target URL" label-width="100px">
              <el-input v-model="info.config.targetDsUrl" size="small" style="width:100%" disabled>
              </el-input>
            </el-form-item>

            <el-row>
              <el-col :span="11">
                <el-form-item label="同步模式" label-width="100px">
                  <el-select v-model="info.config.syncMode" size="small" disabled>
                    <el-option
                        v-for="item in syncMode"
                        :key="item.value"
                        :label="item.label"
                        :value="item.value"/>
                  </el-select>
                </el-form-item>
              </el-col>

            </el-row>

            <el-form-item label="同步表名单" label-width="100px">
              <el-input v-model="info.config.dbTableWhite" size="small" style="width:100%" disabled>
              </el-input>
            </el-form-item>

            <el-form-item label="同步DDL" label-width="100px">
              <el-select v-model="info.config.ddlFilterSet" multiple size="small" style="width:100%" disabled>
                <el-option
                    v-for="item in ddlFilterSet"
                    :key="item.value"
                    :label="item.label"
                    :value="item.value"/>
              </el-select>
            </el-form-item>

            <el-form-item label="预处理集群DDL" label-width="140px" prop="clusterDDL">
              <el-select v-model="info.config.clusterDDL" multiple clearable size="small" style="width:100%" disabled>
                <el-option
                    v-for="item in clusterDDL"
                    :key="item.value"
                    :label="item.label"
                    :value="item.value"/>
              </el-select>
            </el-form-item>

            <el-row>
              <el-col :span="12">
                <el-form-item label="读线程数" label-width="80px" >
                  <el-input v-model.number="info.config.sourceThreadNum" size="small" disabled>
                  </el-input>
                </el-form-item>
              </el-col>
              <el-col :span="12">
                <el-form-item label="写线程" label-width="80px" >
                  <el-input v-model.number="info.config.targetThreadNum" size="small" disabled>
                  </el-input>
                </el-form-item>
              </el-col>
            </el-row>
            <el-row>
              <el-col :span="12">
                <el-form-item label="桶大小" label-width="80px">
                  <el-input v-model.number="info.config.bucketSize" size="small" disabled>
                  </el-input>
                </el-form-item>
              </el-col>
              <el-col :span="12">
                <el-form-item label="缓存区个数" label-width="100px">
                  <el-input v-model.number="info.config.bucketNum" size="small" disabled>
                  </el-input>
                </el-form-item>
              </el-col>
            </el-row>
            <el-row>
              <el-col :span="12">
                <el-form-item label="批数据大小" label-width="100px">
                  <el-input v-model.number="info.config.batchSize" size="small" disabled>
                  </el-input>
                </el-form-item>
              </el-col>
              <el-col :span="12">
                <el-form-item label="oplog开始时间" label-width="120px">
                  <el-date-picker
                      v-model="info.config.startOplogTime * 1000"
                      type="datetime"
                      value-format="timestamp"
                      placeholder="选择日期时间"
                      size="small"
                      disabled
                  >
                  </el-date-picker>
                </el-form-item>
              </el-col>
            </el-row>
            <el-row>
              <el-col :span="12">
                <el-form-item label="oplog结束时间" label-width="120px">
                  <el-date-picker
                      v-model="info.config.endOplogTime"
                      type="datetime"
                      value-format="timestamp"
                      placeholder="选择日期时间"
                      size="small"
                      disabled
                  >
                  </el-date-picker>
                </el-form-item>
              </el-col>
              <el-col :span="12">
                <el-form-item label="oplog延迟时间" label-width="120px">
                  <el-input v-model.number="info.config.delayTime" size="small" disabled>
                  </el-input>
                </el-form-item>
              </el-col>
            </el-row>
            <el-row>
              <el-col :span="12">
                <el-form-item label="实时分桶线程数" label-width="130px" >
                  <el-input v-model.number="info.config.nsBucketThreadNum" size="small" disabled>
                  </el-input>
                </el-form-item>
                <el-form-item label="实时写入线程数" label-width="130px" >
                  <el-input v-model.number="info.config.writeThreadNum" size="small" disabled>
                  </el-input>
                </el-form-item>
              </el-col>
            </el-row>
          </el-form>
        </el-collapse-item>

      </el-collapse>
    </div>
  </div>
</template>

<script>
import {getWorkInfo} from '@/api/taskManagement';

export default {
  props: {
    workName: {
      type: String,
      default: null,
    },
  },
  data() {
    return {
      info: {
        config: {}
      },
      syncMode: [
        {
          label: '全量',
          value: 'all'
        },
        {
          label: '全量和实时',
          value: 'allAndRealTime'
        },
        {
          label: '全量和增量',
          value: 'allAndIncrement'
        },
        {
          label: '实时',
          value: 'realTime'
        },
      ],
      ddlFilterSet: [],
      clusterDDL: [
        {
          label: '删除已经存在的表',
          value: '0'
        },
        {
          label: '打印输出集群全部用户信息',
          value: '1'
        },
        {
          label: '同步库表表结构',
          value: '2'
        },
        {
          label: '同步库表索引信息',
          value: '3'
        },
        {
          label: '全部库开启库分片',
          value: '4'
        },
        {
          label: '同步库表shard key',
          value: '5'
        },
        {
          label: '同步config.setting表',
          value: '6'
        },
        {
          label: '库表预切分chunk',
          value: '7',
        },

      ],
      activeName: '1',
    }
  },

  mounted() {
    this.getWorkInfo()
  },

  methods: {
    getWorkInfo() {
      //获取info
      getWorkInfo(this.workName).then(res => {
        console.log(res);
        this.info.config = res.data
      })
    },

  }
}
</script>
